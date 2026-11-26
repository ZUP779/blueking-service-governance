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

package polaris_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/TencentBlueKing/gopkg/mapx"
	"github.com/TencentBlueKing/gopkg/stringx"
	"github.com/bytedance/mockey"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/config"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/httpcli"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/testutil"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/common/testutil/dbfactory"
	bkmsapp "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/app"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/env"
	bkmsenv "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/core/env/model"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/addon/polaris"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/addon/polaris/accesspoint"
	polarisenvvars "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/addon/polaris/envvars"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/depservice"
	depenvvars "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/depservice/envvars"
	depsvcmodel "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/depservice/model"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/infras/account/auth"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/store"
	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/workload/envvars"
)

// fakePolarisRequest 假北极星收到的一次请求，host 为接入点的客户端地址
type fakePolarisRequest struct {
	Host   string
	Method string
	Path   string
	Token  string
}

// fakeWorkspacePolaris 按路径前缀区分接入点的假北极星：OpenAPIBaseURL 被替换为 {server}/{host}
type fakeWorkspacePolaris struct {
	mu       sync.Mutex
	requests []fakePolarisRequest
}

func (f *fakeWorkspacePolaris) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	host, path, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	path = "/" + path
	f.requests = append(f.requests, fakePolarisRequest{
		Host: host, Method: r.Method, Path: path, Token: r.Header.Get("X-Polaris-Token"),
	})

	write := func(body map[string]any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}
	switch {
	case path == "/naming/v1/services" && r.Method == http.MethodPost:
		write(map[string]any{
			"code":      200000,
			"responses": []map[string]any{{"service": map[string]any{"token": "service-token"}}},
		})
	case path == "/naming/v1/services" && r.Method == http.MethodGet:
		write(map[string]any{
			"code": 200000,
			"services": []map[string]any{{
				"name":      r.URL.Query().Get("name"),
				"namespace": r.URL.Query().Get("namespace"),
				"metadata":  map[string]string{},
			}},
		})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeWorkspacePolaris) received() []fakePolarisRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakePolarisRequest{}, f.requests...)
}

var _ = Describe("Polaris config on workspace access points", func() {
	const (
		tokenA = "token-a"
		tokenB = "token-b"
		hostA  = "10.0.0.1"
		hostB  = "10.0.1.1"
	)

	var (
		ctx              context.Context
		diApp            *fxtest.App
		envService       *env.EnvService
		appDepsVarReader *depenvvars.Reader
		polarisVarReader *polarisenvvars.Reader
		envStateManager  *polaris.PolarisEnvStateManager
		db               *mongo.Database
		configStore      *polaris.ConfigStore
		apStore          *accesspoint.AccessPointStore
		service          *polaris.PolarisConfigService
		app              *bkmsapp.Application
		envA             *bkmsenv.Environment
		envA2            *bkmsenv.Environment
		envB             *bkmsenv.Environment
		envUnbound       *bkmsenv.Environment
		apA              *accesspoint.PolarisAccessPoint
		fakePolaris      *fakeWorkspacePolaris
	)

	createAccessPoint := func(name, host, token string) *accesspoint.PolarisAccessPoint {
		p := &accesspoint.PolarisAccessPoint{
			WorkspaceID: app.WorkspaceID,
			Name:        name + "-" + stringx.Random(6),
			ClientHosts: []string{host, strings.TrimSuffix(host, "1") + "2"},
			Token:       token,
		}
		Expect(apStore.Create(ctx, p)).To(Succeed())
		return p
	}

	bindEnv := func(environment *bkmsenv.Environment, ap *accesspoint.PolarisAccessPoint) {
		Expect(store.Of(db, accesspoint.BindingMeta).Create(ctx, &accesspoint.PolarisClusterBinding{
			WorkspaceID:   app.WorkspaceID,
			ClusterID:     environment.Cluster.ClusterID,
			AccessPointID: ap.ID.Hex(),
		})).To(Succeed())
	}

	importConfig := func(namespace string, scopeEnvNames ...string) *polaris.PolarisConfig {
		return &polaris.PolarisConfig{
			AppID: app.ID,
			Properties: polaris.Properties{
				InstanceKey: "inst", PolarisName: "svc", PolarisNamespace: namespace, ServicePort: 8080,
			},
			ScopeEnvNames: scopeEnvNames,
		}
	}

	buildCR := func(environment *bkmsenv.Environment) map[string]any {
		result, err := polaris.NewWorkloadBuilder(db).Build(ctx, app, environment, nil, corev1.PodSpec{}, nil)
		Expect(err).NotTo(HaveOccurred())
		cr, ok := lo.Find(result.ExtraObjects, func(obj unstructured.Unstructured) bool {
			return obj.GetKind() == "PolarisConfig"
		})
		Expect(ok).To(BeTrue())
		return cr.Object
	}

	BeforeEach(func() {
		ctx = auth.WithUser(testutil.NewDefaultTenantContext(), auth.User{ID: "tester"})
		diApp = fxtest.New(
			GinkgoT(),
			env.FxModule,
			envvars.FxModule,
			depenvvars.FxModule,
			polaris.FxModule,
			polarisenvvars.FxModule,
			fx.Populate(&envService, &appDepsVarReader, &polarisVarReader, &envStateManager),
		)
		diApp.RequireStart()
		DeferCleanup(diApp.RequireStop)

		origin := config.G.Polaris
		config.G.Polaris = config.PolarisConfig{AllowWorkspaceAccessPoints: true}
		DeferCleanup(func() { config.G.Polaris = origin })

		db = testutil.NewDB(GinkgoT())
		configStore = store.Of(db, polaris.ConfigMeta)
		apStore = testutil.NewStore(GinkgoT(), accesspoint.AccessPointMeta)
		service = polaris.NewPolarisConfigService(
			db,
			polaris.NewPolarisPlatformManager(db),
			envStateManager,
			envvars.NewUnifiedEnvVarsReader(db, appDepsVarReader, polarisVarReader),
			nil,
		)

		app = dbfactory.Application(ctx, testutil.NewStore(GinkgoT(), bkmsapp.AppMeta))
		envA = dbfactory.Env(ctx, envService, app.WorkspaceID)
		envA2 = dbfactory.Env(ctx, envService, app.WorkspaceID)
		envB = dbfactory.Env(ctx, envService, app.WorkspaceID)
		envUnbound = dbfactory.Env(ctx, envService, app.WorkspaceID)

		apA = createAccessPoint("ap-a", hostA, tokenA)
		apB := createAccessPoint("ap-b", hostB, tokenB)
		bindEnv(envA, apA)
		bindEnv(envA2, apA)
		bindEnv(envB, apB)

		// 平台创建服务、导入服务都要读取 depservice 中的 polaris 服务方案，接入点的地址与 Token 会覆盖它
		Expect(store.Of(db, depsvcmodel.ServiceMeta).DeleteMany(ctx, depsvcmodel.ByName("polaris"))).Error().
			NotTo(HaveOccurred())
		Expect(store.Of(db, depsvcmodel.ServiceMeta).Create(ctx, &depsvcmodel.Service{
			Name: "polaris",
			Plans: []depsvcmodel.ServicePlan{{
				Name:         "default",
				ProviderType: depsvcmodel.ProviderTypeSystemAllocated,
				Config:       map[string]any{"baseUrl": "http://platform-polaris.invalid"},
			}},
		})).To(Succeed())

		fakePolaris = &fakeWorkspacePolaris{}
		srv := httptest.NewServer(fakePolaris)
		DeferCleanup(srv.Close)
		mockey.Mock(accesspoint.OpenAPIBaseURL).To(func(host string) string { return srv.URL + "/" + host }).Build()
		mockey.Mock(httpcli.CheckDialAddress).Return(nil).Build()
	})

	AfterEach(func() {
		mockey.UnPatchAll()
		_, _ = configStore.DeleteMany(ctx, polaris.ByApp(app.ID))
		Expect(testutil.CleanupMetaColl(accesspoint.AccessPointMeta)).To(Succeed())
		Expect(testutil.CleanupMetaColl(accesspoint.BindingMeta)).To(Succeed())
		Expect(testutil.CleanupMetaColl(depsvcmodel.ServiceInstanceMeta)).To(Succeed())
		_, err := store.Of(db, depsvcmodel.ServiceMeta).DeleteMany(ctx, depsvcmodel.ByName("polaris"))
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Create", func() {
		It("should fix the access point of the scope envs and create the service on it", func() {
			config := importConfig("custom-ns", envA.Name, envA2.Name)
			config.Operator = "owner"
			Expect(service.Create(ctx, app, config, true)).To(Succeed())

			stored, err := configStore.Get(ctx, polaris.ByName(app.ID, config.Name))
			Expect(err).NotTo(HaveOccurred())
			Expect(stored.AccessPointID).To(Equal(apA.ID.Hex()))
			Expect(stored.PolarisToken).To(Equal("service-token"))

			creates := lo.Filter(fakePolaris.received(), func(r fakePolarisRequest, _ int) bool {
				return r.Method == http.MethodPost && r.Path == "/naming/v1/services"
			})
			Expect(creates).To(HaveLen(1))
			Expect(creates[0].Host).To(Equal(hostA))
			Expect(creates[0].Token).To(Equal(tokenA))

			inst, err := depservice.New(db).GetServiceInstance(ctx, stored.DepSvcInstID)
			Expect(err).NotTo(HaveOccurred())
			Expect(inst.Config["accessPointID"]).To(Equal(apA.ID.Hex()))
		})

		It("should reject scope envs on different access points and name the envs", func() {
			err := service.Create(ctx, app, importConfig("custom-ns", envA.Name, envB.Name), false)
			Expect(errors.Is(err, polaris.ErrAccessPointMismatch)).To(BeTrue())
			Expect(polaris.IsClientRequestError(err)).To(BeTrue())
			Expect(err.Error()).To(And(ContainSubstring(envA.Name), ContainSubstring(envB.Name)))
			Expect(configStore.List(ctx, polaris.ByApp(app.ID))).To(BeEmpty())
		})

		It("should reject a scope env whose cluster is not bound and name the env", func() {
			err := service.Create(ctx, app, importConfig("custom-ns", envA.Name, envUnbound.Name), false)
			Expect(errors.Is(err, accesspoint.ErrClusterNotBound)).To(BeTrue())
			Expect(polaris.IsClientRequestError(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring(envUnbound.Name))
		})

		It("should require scope envs when the platform access point is unavailable", func() {
			err := service.Create(ctx, app, importConfig("custom-ns"), false)
			Expect(errors.Is(err, polaris.ErrScopeEnvRequired)).To(BeTrue())
			Expect(polaris.IsClientRequestError(err)).To(BeTrue())
		})

		It("should reject the weight factor", func() {
			config := importConfig("custom-ns", envA.Name)
			config.EnableWeightFactor = true
			err := service.Create(ctx, app, config, false)
			Expect(errors.Is(err, polaris.ErrWeightFactorUnsupported)).To(BeTrue())
			Expect(polaris.IsClientRequestError(err)).To(BeTrue())
		})
	})

	Describe("Update", func() {
		var config *polaris.PolarisConfig

		BeforeEach(func() {
			config = importConfig("custom-ns", envA.Name)
			Expect(service.Create(ctx, app, config, false)).To(Succeed())
		})

		It("should reject adding an env bound to another access point", func() {
			_, err := service.Update(ctx, app, config, &polaris.ConfigUpdateData{
				ScopeEnvNames: []string{envA.Name, envB.Name},
			})
			Expect(errors.Is(err, polaris.ErrAccessPointMismatch)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring(envB.Name))

			stored, getErr := configStore.Get(ctx, polaris.ByName(app.ID, config.Name))
			Expect(getErr).NotTo(HaveOccurred())
			Expect(stored.ScopeEnvNames).To(Equal([]string{envA.Name}))
		})

		It("should allow adding an env bound to the same access point", func() {
			updated, err := service.Update(ctx, app, config, &polaris.ConfigUpdateData{
				ScopeEnvNames: []string{envA.Name, envA2.Name},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.ScopeEnvNames).To(ConsistOf(envA.Name, envA2.Name))
		})

		It("should reject turning the weight factor on and ignore turning it off", func() {
			_, err := service.Update(ctx, app, config, &polaris.ConfigUpdateData{EnableWeightFactor: lo.ToPtr(true)})
			Expect(errors.Is(err, polaris.ErrWeightFactorUnsupported)).To(BeTrue())

			_, err = service.Update(ctx, app, config, &polaris.ConfigUpdateData{EnableWeightFactor: lo.ToPtr(false)})
			Expect(err).NotTo(HaveOccurred())
			Expect(fakePolaris.received()).To(BeEmpty())
		})

		It("should reject turning dynamic weight on", func() {
			_, err := service.UpdateEnvWeight(ctx, app, config, envA.Name, 50, lo.ToPtr(true))
			Expect(errors.Is(err, polaris.ErrWeightFactorUnsupported)).To(BeTrue())
			Expect(polaris.IsClientRequestError(err)).To(BeTrue())
		})
	})

	Describe("Workload", func() {
		It("should use the current access point token in the CR and omit dynamic weight", func() {
			// 引入已有服务不需要服务 Token
			Expect(service.Create(ctx, app, importConfig("custom-ns", envA.Name), false)).To(Succeed())

			cr := buildCR(envA)
			Expect(mapx.GetStr(cr, "spec.polaris.token")).To(Equal(tokenA))
			Expect(mapx.GetMap(cr, "spec.polaris")).NotTo(HaveKey("dynamicWeight"))

			stored, err := apStore.Get(ctx, store.ByID(apA.ID))
			Expect(err).NotTo(HaveOccurred())
			stored.Token = "token-a-rotated"
			Expect(apStore.Replace(ctx, stored, store.ByID(apA.ID))).To(Succeed())

			Expect(mapx.GetStr(buildCR(envA), "spec.polaris.token")).To(Equal("token-a-rotated"))
		})
	})

	Describe("Imported service", func() {
		It("should read the service on the access point without a service token", func() {
			svc, err := service.GetImportedService(ctx, app, apA.ID.Hex(), "svc", "custom-ns", "")
			Expect(err).NotTo(HaveOccurred())
			Expect(svc.Name).To(Equal("svc"))

			requests := fakePolaris.received()
			Expect(requests).To(HaveEach(HaveField("Host", hostA)))
			Expect(requests).To(HaveEach(HaveField("Token", tokenA)))
			Expect(requests).To(HaveEach(HaveField("Method", http.MethodGet)))
		})

		It("should reject writing the weight factor", func() {
			err := service.UpdateImportedPolaris(ctx, app, apA.ID.Hex(), "svc", "custom-ns", "", true)
			Expect(errors.Is(err, polaris.ErrWeightFactorUnsupported)).To(BeTrue())
		})

		It("should reject an access point of another workspace", func() {
			other := dbfactory.Application(ctx, testutil.NewStore(GinkgoT(), bkmsapp.AppMeta))
			_, err := service.GetImportedService(ctx, other, apA.ID.Hex(), "svc", "custom-ns", "")
			Expect(errors.Is(err, accesspoint.ErrAccessPointNotFound)).To(BeTrue())
			Expect(polaris.IsClientRequestError(err)).To(BeTrue())
		})
	})

	Describe("AccessPointNames", func() {
		It("should map access point IDs to names", func() {
			config := importConfig("custom-ns", envA.Name)
			Expect(service.Create(ctx, app, config, false)).To(Succeed())

			names := service.AccessPointNames(ctx, app, []*polaris.PolarisConfig{config})
			Expect(names).To(Equal(map[string]string{apA.ID.Hex(): apA.Name}))
		})
	})
})
