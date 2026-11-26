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

	"github.com/pkg/errors"

	log "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/logging"
	bkmsapp "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/app"
	bkmsenv "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/env/model"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/addon/polaris/accesspoint"
)

// resolveEnvsAccessPoint 按环境所在集群的绑定解析接入点，并校验所有环境是否解析出同一个接入点；envs 为空时返回 nil。
// 集群未绑定时返回 accesspoint.ErrClusterNotBound，环境之间接入点不一致时返回 ErrAccessPointMismatch，错误信息中带上环境名
func (s *PolarisConfigService) resolveEnvsAccessPoint(
	ctx context.Context, workspaceID string, envs []bkmsenv.Environment,
) (*accesspoint.AccessPoint, error) {
	var (
		resolved *accesspoint.AccessPoint
		firstEnv string
	)
	byCluster := make(map[string]*accesspoint.AccessPoint, len(envs))
	for i := range envs {
		env := &envs[i]
		ap, ok := byCluster[env.Cluster.ClusterID]
		if !ok {
			var err error
			if ap, err = s.resolver.ResolveByCluster(ctx, workspaceID, env.Cluster.ClusterID); err != nil {
				return nil, errors.Wrapf(err, "resolve polaris access point of env %s", env.Name)
			}
			byCluster[env.Cluster.ClusterID] = ap
		}
		if resolved == nil {
			resolved, firstEnv = ap, env.Name
			continue
		}
		if ap.ID != resolved.ID {
			return nil, errors.Wrapf(ErrAccessPointMismatch,
				"env %s uses polaris access point %q but env %s uses %q", env.Name, ap.Name, firstEnv, resolved.Name)
		}
	}
	return resolved, nil
}

// validateNamespace 校验接入点允许使用该北极星命名空间，见 accesspoint.AccessPoint.AllowsNamespace
func validateNamespace(ap *accesspoint.AccessPoint, namespace string) error {
	if !ap.AllowsNamespace(namespace) {
		return errors.Wrapf(ErrInvalidNamespace,
			"namespace %q is not allowed on polaris access point %q", namespace, ap.Name)
	}
	return nil
}

// crToken PolarisConfig CR 中 operator 访问北极星所用的 Token：接入点要求服务 Token 时使用配置的服务 Token；
// 否则使用接入点的 Token，每次下发时实时解析，因此更换接入点 Token 后下次部署或绑定即生效
func crToken(cfg *PolarisConfig, ap *accesspoint.AccessPoint) string {
	if ap.Capabilities.ServiceTokenRequired {
		return cfg.PolarisToken
	}
	return ap.Token
}

// AccessPointNames 返回 polarisConfig 所用接入点 ID 到接入点名称的映射。
// 平台托管接入点不查库；解析失败时名称为空，只记录日志，不影响配置的展示
func (s *PolarisConfigService) AccessPointNames(
	ctx context.Context, app *bkmsapp.Application, configs []*PolarisConfig,
) map[string]string {
	names := make(map[string]string, 1)
	for _, cfg := range configs {
		id := cfg.EffectiveAccessPointID()
		if _, ok := names[id]; ok {
			continue
		}
		if accesspoint.KindOfID(id) == accesspoint.KindPlatform {
			names[id] = accesspoint.PlatformName
			continue
		}
		ap, err := s.resolver.ResolveByID(ctx, app.WorkspaceID, id)
		if err != nil {
			log.Errorf(ctx, "resolve polaris access point %s of app %s failed: %v", id, app.ID, err)
			names[id] = ""
			continue
		}
		names[id] = ap.Name
	}
	return names
}
