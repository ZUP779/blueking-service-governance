/*
 * TencentBlueKing is pleased to support the open source community by making
 * 蓝鲸智云 - 服务治理 (BlueKing Service Governance) available.
 * Copyright (C) Tencent. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We undertake not to change the open source license (MIT license) applicable
 * to the current version of the project delivered to anyone in the future.
 */

package polaris

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	log "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/logging"
	bkmsapp "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/app"
	bkmsenv "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/env/model"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/resname"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/addon/polaris/accesspoint"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/store"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/workload/appmodelcore/appmodel"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/workload/appmodelcore/appspec"
)

// DynamicApplyEnqueuer 为单个可动态下发环境投递 asynq 任务。
type DynamicApplyEnqueuer func(ctx context.Context, appID, configName, envName string) error

// ErrClusterSyncFailed 表示配置本身已经保存成功，但 immediate 模式的集群资源同步失败。
//
// 调用方据此区分"操作没生效"和"配置已保存、北极星侧未收敛"：后者的配置变更依然有效，
// 失败原因已写入对应环境的 EnvState.LastError，重新保存一次配置即可重试。
var ErrClusterSyncFailed = errors.New("polaris cluster resources sync failed")

// PolarisConfigService 负责配置管理和北极星平台服务生命周期。
// on_deploy 的动态下发通过 asynq 按环境入队；immediate 配置在请求内同步下发 CR 与 Service。
//
// 配置创建时由生效环境的集群绑定推导出接入点并固定，之后只允许加入集群绑定到同一接入点的环境；
// 权重因子、命名空间、服务 Token 等差异按接入点能力处理，见 accesspoint.Capabilities
type PolarisConfigService struct {
	db                  *mongo.Database
	platformManager     *PolarisPlatformManager
	envStateManager     *PolarisEnvStateManager
	applier             *CRApplier
	resolver            accesspoint.Resolver
	envStore            *bkmsenv.EnvironmentStore
	envVarsReader       dynamicApplyEnvVarsReader
	enqueueDynamicApply DynamicApplyEnqueuer
}

// NewPolarisConfigService 创建北极星配置服务。
func NewPolarisConfigService(
	db *mongo.Database,
	platformManager *PolarisPlatformManager,
	envStateManager *PolarisEnvStateManager,
	envVarsReader dynamicApplyEnvVarsReader,
	enqueue DynamicApplyEnqueuer,
) *PolarisConfigService {
	return &PolarisConfigService{
		db:                  db,
		platformManager:     platformManager,
		envStateManager:     envStateManager,
		applier:             NewCRApplier(db),
		resolver:            accesspoint.NewResolver(db),
		envStore:            store.Of(db, bkmsenv.EnvMeta),
		envVarsReader:       envVarsReader,
		enqueueDynamicApply: enqueue,
	}
}

// Create 创建北极星配置，并按需创建北极星服务实例。
//
// 配置的接入点由生效环境的集群绑定推导：所有环境必须解析出同一个接入点，
// 生效范围为空时使用平台托管接入点（不可用时返回 ErrScopeEnvRequired）
func (s *PolarisConfigService) Create(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
	createNewService bool,
) error {
	// 提前生成配置名，以便在创建北极星服务前校验资源名长度
	if config.Name == "" {
		config.Name = config.GenerateName()
	}
	envs, err := s.listScopeEnvs(ctx, app, config.ScopeEnvNames)
	if err != nil {
		return err
	}
	if err = checkResourceNamesInEnvs(app, config.Name, envs); err != nil {
		return err
	}
	ap, err := s.resolveCreateAccessPoint(ctx, app, envs)
	if err != nil {
		return err
	}
	if err = validateForAccessPoint(ap, config, createNewService); err != nil {
		return err
	}
	config.AccessPointID = ap.ID

	if createNewService {
		result, err := s.platformManager.CreateService(ctx, &CreatePolarisServiceParams{
			AccessPoint:        ap,
			PolarisName:        config.PolarisName,
			PolarisNamespace:   config.PolarisNamespace,
			Operator:           config.Operator,
			WorkspaceID:        app.WorkspaceID,
			AppID:              app.ID,
			ScopeEnvNames:      config.ScopeEnvNames,
			EnableWeightFactor: config.EnableWeightFactor,
		})
		if err != nil {
			return errors.Wrap(err, "create polaris service")
		}
		config.PolarisToken = result.Token
		config.DepSvcInstID = result.ServiceInstanceID
	}

	// 过滤掉 scope 外且未部署的环境级设置，并为 scope 内未设置权重的环境补充默认值
	config.EnvWeights, config.EnvDynamicWeights = s.envStateManager.reconcileEnvSettingsForScope(
		config.ScopeEnvNames, config.EnvWeights, config.EnvDynamicWeights, nil, config.RegisterMode,
	)

	// 权重因子只存在于北极星。平台创建已在上面写入；导入路径忽略请求里的值。
	config.EnableWeightFactor = false
	if err := store.Of(s.db, ConfigMeta).Create(ctx, config); err != nil {
		return err
	}

	// immediate 配置绑定即注册，在请求内同步下发；配置已经落库，失败时由调用方决定是否重试
	if !config.IsImmediateRegister() {
		return nil
	}
	return s.applyImmediately(ctx, app, config, config.ScopeEnvNames)
}

// resolveCreateAccessPoint 推导新建配置使用的接入点：生效范围内的环境必须解析出同一个接入点；
// 生效范围为空时只能使用平台托管接入点，不可用（只允许空间自建接入点）时返回 ErrScopeEnvRequired
func (s *PolarisConfigService) resolveCreateAccessPoint(
	ctx context.Context, app *bkmsapp.Application, envs []bkmsenv.Environment,
) (*accesspoint.AccessPoint, error) {
	ap, err := s.resolveEnvsAccessPoint(ctx, app.WorkspaceID, envs)
	if err != nil || ap != nil {
		return ap, err
	}
	ap, err = s.resolver.ResolveByID(ctx, app.WorkspaceID, accesspoint.PlatformID)
	if errors.Is(err, accesspoint.ErrAccessPointNotFound) {
		return nil, errors.Wrap(ErrScopeEnvRequired, "platform polaris access point is unavailable")
	}
	if err != nil {
		return nil, errors.Wrap(err, "resolve platform polaris access point")
	}
	return ap, nil
}

// validateForAccessPoint 按接入点能力校验新建配置：权重因子、引入已有服务时的服务 Token、命名空间
func validateForAccessPoint(ap *accesspoint.AccessPoint, config *PolarisConfig, createNewService bool) error {
	if config.EnableWeightFactor && !ap.Capabilities.WeightFactor {
		return errors.Wrapf(ErrWeightFactorUnsupported, "polaris access point %q", ap.Name)
	}
	if !createNewService && ap.Capabilities.ServiceTokenRequired && config.PolarisToken == "" {
		return ErrServiceTokenRequired
	}
	return validateNamespace(ap, config.PolarisNamespace)
}

// ResolveAccessPoint 解析应用所在空间的接入点，accessPointID 为空时表示平台托管接入点。
// 用于创建配置前导入已有服务等还没有配置的场景
func (s *PolarisConfigService) ResolveAccessPoint(
	ctx context.Context, app *bkmsapp.Application, accessPointID string,
) (*accesspoint.AccessPoint, error) {
	accessPointID = lo.CoalesceOrEmpty(accessPointID, accesspoint.PlatformID)
	ap, err := s.resolver.ResolveByID(ctx, app.WorkspaceID, accessPointID)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve polaris access point %s", accessPointID)
	}
	return ap, nil
}

// GetImportedService 查询接入点上要引入的已有北极星服务信息。
// 接入点要求服务 Token 时 token 必填并校验可写，否则忽略 token
func (s *PolarisConfigService) GetImportedService(
	ctx context.Context,
	app *bkmsapp.Application,
	accessPointID, name, namespace, token string,
) (*RemotePolarisService, error) {
	ap, err := s.ResolveAccessPoint(ctx, app, accessPointID)
	if err != nil {
		return nil, err
	}
	if ap.Capabilities.ServiceTokenRequired && token == "" {
		return nil, ErrServiceTokenRequired
	}
	if err = validateNamespace(ap, namespace); err != nil {
		return nil, err
	}
	return s.platformManager.GetImportedService(ctx, ap, name, namespace, token)
}

// UpdateImportedPolaris 修改接入点上已有的北极星服务。目前只写权重因子开关，不回写本地配置；
// 接入点不支持权重因子时返回 ErrWeightFactorUnsupported
func (s *PolarisConfigService) UpdateImportedPolaris(
	ctx context.Context,
	app *bkmsapp.Application,
	accessPointID, name, namespace, token string,
	enabled bool,
) error {
	ap, err := s.ResolveAccessPoint(ctx, app, accessPointID)
	if err != nil {
		return err
	}
	if !ap.Capabilities.WeightFactor {
		return errors.Wrapf(ErrWeightFactorUnsupported, "polaris access point %q", ap.Name)
	}
	if err = validateNamespace(ap, namespace); err != nil {
		return err
	}
	return s.platformManager.UpdateImportedService(
		ctx, ap, name, namespace, token, &UpdateServiceParams{EnableWeightFactor: &enabled},
	)
}

// GetRemoteServices 按配置名返回对应的北极星线上服务，同一接入点上的 namespace/name 只查一次。
// 查不到的配置对应值为 nil，不中断其余配置。
func (s *PolarisConfigService) GetRemoteServices(
	ctx context.Context,
	app *bkmsapp.Application,
	configs []*PolarisConfig,
) map[string]*RemotePolarisService {
	type serviceKey struct {
		accessPointID string
		namespace     string
		name          string
	}
	cached := make(map[serviceKey]*RemotePolarisService, len(configs))
	out := make(map[string]*RemotePolarisService, len(configs))
	for _, config := range configs {
		if config == nil || config.Name == "" {
			continue
		}
		key := serviceKey{
			accessPointID: config.EffectiveAccessPointID(),
			namespace:     config.PolarisNamespace,
			name:          config.PolarisName,
		}
		svc, ok := cached[key]
		if !ok {
			svc = s.getRemoteService(ctx, app, config)
			cached[key] = svc
		}
		out[config.Name] = svc
	}
	return out
}

// getRemoteService 查询配置对应的北极星线上服务，失败时记录日志并返回 nil
func (s *PolarisConfigService) getRemoteService(
	ctx context.Context, app *bkmsapp.Application, config *PolarisConfig,
) *RemotePolarisService {
	ap, err := s.resolver.ResolveByID(ctx, app.WorkspaceID, config.EffectiveAccessPointID())
	if err == nil {
		var svc *RemotePolarisService
		if svc, err = s.platformManager.GetService(ctx, ap, config.PolarisName, config.PolarisNamespace); err == nil {
			return svc
		}
	}
	log.Errorf(ctx, "get polaris service failed, polaris=%s/%s: %v",
		config.PolarisNamespace, config.PolarisName, err)
	return nil
}

// Update 更新北极星配置
func (s *PolarisConfigService) Update(
	ctx context.Context,
	app *bkmsapp.Application,
	oldConfig *PolarisConfig,
	updateData *ConfigUpdateData,
) (*PolarisConfig, error) {
	if updateData.ScopeEnvNames != nil {
		if err := s.checkScopeUpdate(ctx, app, oldConfig, updateData.ScopeEnvNames); err != nil {
			return nil, err
		}
	}
	if updateData.EnableWeightFactor != nil && !oldConfig.Capabilities().WeightFactor {
		if *updateData.EnableWeightFactor {
			return nil, ErrWeightFactorUnsupported
		}
		// 不支持权重因子的接入点上本来就没有开启，关闭请求无需写回北极星
		updateData = lo.ToPtr(*updateData)
		updateData.EnableWeightFactor = nil
	}
	if err := s.syncPolarisService(ctx, oldConfig, updateData); err != nil {
		return nil, err
	}

	if updateData.ScopeEnvNames != nil {
		// scope 变化时保留仍有效的环境级设置，并为新增环境补充默认权重。
		updateData.envWeights, updateData.envDynamicWeights = s.envStateManager.reconcileEnvSettingsForScope(
			updateData.ScopeEnvNames,
			oldConfig.EnvWeights,
			oldConfig.EnvDynamicWeights,
			oldConfig.EnvStates,
			oldConfig.RegisterMode,
		)
	}

	if err := UpdateConfig(ctx, s.db, app.ID, oldConfig.Name, updateData); err != nil {
		return nil, errors.Wrap(err, "update polaris config")
	}

	newConfig, err := store.Of(s.db, ConfigMeta).Get(ctx, ByName(app.ID, oldConfig.Name))
	if err != nil {
		return nil, errors.Wrap(err, "get updated polaris config")
	}
	if !updateData.affectsWorkload() {
		return newConfig, nil
	}

	if newConfig.IsImmediateRegister() {
		reconcileErr := s.reconcileImmediately(ctx, app, newConfig)
		// 同步下发会改写环境记录，重新读取以便调用方拿到本次下发后的状态
		syncedConfig, getErr := store.Of(s.db, ConfigMeta).Get(ctx, ByName(app.ID, oldConfig.Name))
		if getErr != nil {
			return newConfig, stderrors.Join(
				reconcileErr, errors.Wrap(getErr, "get polaris config after immediate apply"),
			)
		}
		return syncedConfig, reconcileErr
	}

	envNames, err := s.envStateManager.PrepareDynamicApply(ctx, newConfig)
	if err != nil {
		return newConfig, errors.Wrap(err, "prepare dynamic polaris apply")
	}
	// 请求结束后仍需保证任务投递，避免客户端断开导致配置更新成功但任务丢失。
	if err = s.triggerDynamicApply(context.WithoutCancel(ctx), newConfig, envNames); err != nil {
		return newConfig, errors.Wrap(err, "enqueue polaris dynamic apply")
	}
	return newConfig, nil
}

// Delete 删除北极星配置，并按需删除北极星服务实例
func (s *PolarisConfigService) Delete(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
) error {
	// immediate 配置的集群资源不会随部署清理，必须在删除配置前先摘除，否则实例会一直留在北极星上。
	// 删除失败时不继续删配置，用户重试即可收敛。
	if config.IsImmediateRegister() {
		if err := s.releaseFromEnvs(ctx, app, config, config.TrackedEnvNames()); err != nil {
			return err
		}
	}

	if !config.DepSvcInstID.IsZero() {
		if err := s.platformManager.DeleteService(ctx, &DeleteServiceParams{
			ServiceInstanceID: config.DepSvcInstID,
			AppID:             app.ID,
		}); err != nil {
			return errors.Wrap(err, "delete polaris service")
		}
	}
	if err := store.Of(s.db, ConfigMeta).Delete(ctx, ByName(app.ID, config.Name)); err != nil {
		return errors.Wrap(err, "delete polaris config")
	}
	return nil
}

// checkScopeUpdate 校验新的生效范围：各环境中的资源名不超长，新加入的环境的集群必须绑定到配置固定的接入点
func (s *PolarisConfigService) checkScopeUpdate(
	ctx context.Context, app *bkmsapp.Application, config *PolarisConfig, scopeEnvNames []string,
) error {
	envs, err := s.listScopeEnvs(ctx, app, scopeEnvNames)
	if err != nil {
		return err
	}
	if err = checkResourceNamesInEnvs(app, config.Name, envs); err != nil {
		return err
	}
	added := lo.Filter(envs, func(env bkmsenv.Environment, _ int) bool {
		return !config.IsAvailableInEnv(env.Name)
	})
	ap, err := s.resolveEnvsAccessPoint(ctx, app.WorkspaceID, added)
	if err != nil {
		return err
	}
	if ap != nil && ap.ID != config.EffectiveAccessPointID() {
		return errors.Wrapf(ErrAccessPointMismatch,
			"envs %v use polaris access point %q, but the config uses another one",
			lo.Map(added, func(env bkmsenv.Environment, _ int) string { return env.Name }), ap.Name)
	}
	return nil
}

// listScopeEnvs 列出生效范围内应用可用的环境，不存在的环境名被忽略
func (s *PolarisConfigService) listScopeEnvs(
	ctx context.Context, app *bkmsapp.Application, envNames []string,
) ([]bkmsenv.Environment, error) {
	if len(envNames) == 0 {
		return nil, nil
	}
	envs, err := s.envStore.List(ctx,
		bkmsenv.InWorkspace(app.WorkspaceID),
		bkmsenv.AvailableToApp(app.ID),
		store.Where("name", bson.M{"$in": envNames}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "list scope envs")
	}
	return envs, nil
}

// checkResourceNamesInEnvs 校验配置在生效范围内各环境中的 CR 名与 Service 名不超长。
// 共用命名空间的环境创建时只校验应用自身的资源名，北极星资源名带 resourcePrefix 前缀后更长，在加入生效范围时校验。
func checkResourceNamesInEnvs(app *bkmsapp.Application, configName string, envs []bkmsenv.Environment) error {
	for i := range envs {
		crName, serviceName := PolarisResourceNames(&envs[i], app.Name, configName)
		if err := checkResourceNames(&envs[i], crName, serviceName); err != nil {
			return err
		}
	}
	return nil
}

// checkResourceNames 校验环境中生成的 PolarisConfig CR 名与配套 Service 名合法性。
func checkResourceNames(env *bkmsenv.Environment, names ...string) error {
	for _, name := range names {
		if err := resname.Validate(name); err != nil {
			return errors.Wrapf(
				ErrInvalidResourceName, "resource name %s in env %s: %v", name, env.Name, err,
			)
		}
	}
	return nil
}

// reconcileImmediately 同步收敛 immediate 配置的集群资源：先清理离域环境，再下发全部 scope 环境。
func (s *PolarisConfigService) reconcileImmediately(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
) error {
	releaseErr := s.releaseFromEnvs(
		ctx, app, config, config.EnvNamesOutsideScope(),
	)
	applyErr := s.applyImmediately(ctx, app, config, config.ScopeEnvNames)
	if releaseErr != nil {
		return releaseErr
	}
	return applyErr
}

// applyImmediately 逐环境同步下发 CR 与 Service，并记录每个环境的结果。
// 单个环境失败不影响其余环境，全部处理完后汇总返回。
func (s *PolarisConfigService) applyImmediately(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
	envNames []string,
) error {
	if len(envNames) == 0 {
		return nil
	}

	appModel, err := store.Of(s.db, appmodel.AppModelMeta).Get(ctx, appmodel.ByApp(app.ID))
	if err != nil {
		applyErr := errors.Wrap(err, "get app model for polaris resources apply")
		log.Errorf(ctx, "get app model for polaris resources apply failed, app=%s: %v", app.ID, applyErr)
		return errors.Wrap(ErrClusterSyncFailed, applyErr.Error())
	}

	failures := make([]string, 0, len(envNames))
	for _, envName := range envNames {
		applyErr := s.applyResourcesToEnv(ctx, app, appModel, config, envName)
		s.recordImmediateApplyResult(ctx, config, envName, applyErr)
		if applyErr == nil {
			continue
		}
		log.Errorf(ctx, "apply polaris resources failed, app=%s config=%s env=%s: %v",
			app.ID, config.Name, envName, applyErr)
		failures = append(failures, fmt.Sprintf("%s: %v", envName, applyErr))
	}
	if len(failures) > 0 {
		return errors.Wrapf(ErrClusterSyncFailed, "apply failed in env(s) [%s]", strings.Join(failures, "; "))
	}
	return nil
}

// applyResourcesToEnv 读取单个环境的构建输入并下发该配置的全部资源。
func (s *PolarisConfigService) applyResourcesToEnv(
	ctx context.Context,
	app *bkmsapp.Application,
	appModel *appmodel.AppModel,
	config *PolarisConfig,
	envName string,
) error {
	env, err := s.envStore.Get(ctx, bkmsenv.AppEnvByName(app.WorkspaceID, app.ID, envName))
	if err != nil {
		return errors.Wrapf(err, "get env %s", envName)
	}
	effective, _, err := appspec.ModelForEnv(ctx, s.db, app.ID, env.Name, appModel)
	if err != nil {
		return errors.Wrapf(err, "apply env app spec for %s", envName)
	}
	envVars, err := s.envVarsReader.ListVars(ctx, *env, app, effective)
	if err != nil {
		return errors.Wrapf(err, "build env vars for %s", envName)
	}
	return s.applier.Apply(ctx, app, env, config, envVars.ToMap())
}

// releaseFromEnvs 逐环境删除集群资源，成功后清理该环境的记录与权重。
// 删除失败时保留记录，下次保存配置会重新进入清理列表。
func (s *PolarisConfigService) releaseFromEnvs(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
	envNames []string,
) error {
	failures := make([]string, 0, len(envNames))
	for _, envName := range envNames {
		releaseErr := s.releaseFromEnv(ctx, app, config, envName)
		if releaseErr == nil {
			continue
		}
		log.Errorf(ctx, "release polaris resources failed, app=%s config=%s env=%s: %v",
			app.ID, config.Name, envName, releaseErr)
		s.recordImmediateApplyResult(ctx, config, envName, releaseErr)
		failures = append(failures, fmt.Sprintf("%s: %v", envName, releaseErr))
	}
	if len(failures) > 0 {
		return errors.Wrapf(ErrClusterSyncFailed, "release failed in env(s) [%s]", strings.Join(failures, "; "))
	}
	return nil
}

func (s *PolarisConfigService) releaseFromEnv(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
	envName string,
) error {
	env, err := s.envStore.Get(ctx, bkmsenv.AppEnvByName(app.WorkspaceID, app.ID, envName))
	if err != nil {
		return errors.Wrapf(err, "get env %s", envName)
	}
	if err = s.applier.DeleteResources(ctx, app, env, config); err != nil {
		return err
	}
	return s.envStateManager.ReleaseEnv(ctx, app.ID, config.Name, envName)
}

func (s *PolarisConfigService) recordImmediateApplyResult(
	ctx context.Context,
	config *PolarisConfig,
	envName string,
	applyErr error,
) {
	if err := s.envStateManager.RecordImmediateApplyResult(ctx, config, envName, applyErr); err != nil {
		log.Errorf(ctx, "record polaris resources apply result failed, app=%s config=%s env=%s: %v",
			config.AppID, config.Name, envName, err)
	}
}

// triggerDynamicApply 为每个可下发环境投递一条 asynq 任务。
// 某个环境投递失败会写入该环境 LastError，不阻止其余环境入队；返回全部投递错误的汇总。
func (s *PolarisConfigService) triggerDynamicApply(
	ctx context.Context,
	config *PolarisConfig,
	envNames []string,
) error {
	var errs []error
	for _, envName := range envNames {
		if err := s.enqueueDynamicApply(ctx, config.AppID, config.Name, envName); err != nil {
			enqueueErr := errors.Wrapf(err, "enqueue polaris dynamic apply for env %s", envName)
			if recErr := s.envStateManager.RecordDynamicApplyResult(
				ctx, config.AppID, config.Name, envName, config.UpdatedAt, enqueueErr,
			); recErr != nil {
				log.Errorf(ctx, "record polaris enqueue failure failed, app=%s config=%s env=%s: %v",
					config.AppID, config.Name, envName, recErr)
			}
			errs = append(errs, enqueueErr)
		}
	}
	return stderrors.Join(errs...)
}

func (s *PolarisConfigService) patchEnvWeight(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
	envName string,
	weight int32,
	dynamicWeight bool,
) error {
	env, err := s.envStore.Get(ctx, bkmsenv.AppEnvByName(app.WorkspaceID, app.ID, envName))
	if err != nil {
		return errors.Wrapf(err, "get env %s", envName)
	}
	return s.applier.PatchWeight(ctx, app, env, config, weight, dynamicWeight)
}

// UpdateEnvWeight 更新指定环境的北极星实例权重与动态权重开关；
// dynamicWeight 为 nil 表示只调权重、不动开关。
// 已部署环境会先同步 Patch 集群资源，成功后再持久化。
func (s *PolarisConfigService) UpdateEnvWeight(
	ctx context.Context,
	app *bkmsapp.Application,
	config *PolarisConfig,
	envName string,
	weight int32,
	dynamicWeight *bool,
) (*PolarisConfig, error) {
	if lo.FromPtr(dynamicWeight) && !config.Capabilities().WeightFactor {
		return nil, errors.Wrap(ErrWeightFactorUnsupported, "dynamic weight requires weight factor")
	}
	isDeployed := config.GetEnvState(envName).IsDeployed()
	if isDeployed {
		if err := s.patchEnvWeight(
			ctx, app, config, envName, weight,
			lo.FromPtrOr(dynamicWeight, config.EnvDynamicWeights[envName]),
		); err != nil {
			log.Errorf(ctx, "patch polaris CR weight failed, app=%s config=%s env=%s: %v",
				app.ID, config.Name, envName, err)
			return nil, errors.Wrap(err, "patch env weight")
		}
	}

	if err := UpsertEnvWeight(
		ctx, s.db, app.ID, config.Name, envName, weight, dynamicWeight,
	); err != nil {
		if isDeployed {
			log.Errorf(ctx, "persist polaris env weight after cluster patch failed, app=%s config=%s env=%s: %v",
				app.ID, config.Name, envName, err)
		}
		return nil, errors.Wrap(err, "update env weight")
	}

	// 重新读取最新配置
	newConfig, err := store.Of(s.db, ConfigMeta).Get(ctx, ByName(app.ID, config.Name))
	if err != nil {
		return nil, errors.Wrap(err, "get updated polaris config")
	}

	return newConfig, nil
}

// syncPolarisService 配置保存时把负责人、权重因子写回北极星。
// 平台创建走依赖服务实例；从现有引入只在请求带了权重因子时用 Token 写入，不改负责人。
func (s *PolarisConfigService) syncPolarisService(
	ctx context.Context,
	oldConfig *PolarisConfig,
	updateData *ConfigUpdateData,
) error {
	if updateData.Operator != nil && strings.TrimSpace(*updateData.Operator) == "" {
		return ErrOperatorEmpty
	}
	if oldConfig.DepSvcInstID.IsZero() {
		if updateData.Operator != nil {
			return ErrNotManaged
		}
		if updateData.EnableWeightFactor == nil {
			return nil
		}
		token := oldConfig.PolarisToken
		if updateData.PolarisToken != nil && *updateData.PolarisToken != "" {
			token = *updateData.PolarisToken
		}
		ap, err := s.resolver.ResolveForPolarisConfig(ctx, oldConfig)
		if err != nil {
			return errors.Wrap(err, "resolve polaris access point of config")
		}
		return s.platformManager.UpdateImportedService(
			ctx, ap, oldConfig.PolarisName, oldConfig.PolarisNamespace, token,
			&UpdateServiceParams{EnableWeightFactor: updateData.EnableWeightFactor},
		)
	}
	if updateData.Operator == nil && updateData.EnableWeightFactor == nil {
		return nil
	}
	if err := s.platformManager.UpdateService(ctx, oldConfig, &UpdateServiceParams{
		Owners:             updateData.Operator,
		EnableWeightFactor: updateData.EnableWeightFactor,
	}); err != nil {
		return errors.Wrap(err, "update polaris service")
	}
	return nil
}
