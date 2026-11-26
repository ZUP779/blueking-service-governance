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
	"github.com/pkg/errors"

	"github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/addon/polaris/accesspoint"
	polarisprovider "github.com/TencentBlueKing/blueking-service-governance/bkms-server/pkg/extension/depservice/provider/polaris"
)

var (
	// ErrConfigNotFound 北极星配置不存在
	ErrConfigNotFound = errors.New("polaris config not found")
	// ErrConfigNameExists 北极星配置名称已存在
	ErrConfigNameExists = errors.New("polaris config name already exists")
	// ErrOperatorEmpty 不允许将负责人清空
	ErrOperatorEmpty = errors.New("operator cannot be empty")
	// ErrNotManaged 部分字段仅平台创建的北极星服务允许修改
	ErrNotManaged = errors.New("field can only be set for platform-created polaris services")
	// ErrInvalidResourceName 生成的集群资源名不符合 K8s 命名规则（如超长）
	ErrInvalidResourceName = errors.New("invalid polaris resource name")
	// ErrAccessPointMismatch 生效环境的集群绑定了不同的北极星接入点，或与配置固定的接入点不一致
	ErrAccessPointMismatch = errors.New("scope envs must use the same polaris access point")
	// ErrScopeEnvRequired 平台托管接入点不可用时，必须选择生效环境才能确定配置使用的接入点
	ErrScopeEnvRequired = errors.New("scope envs are required to determine the polaris access point")
	// ErrInvalidNamespace 北极星命名空间不在接入点可选的范围内
	ErrInvalidNamespace = errors.New("invalid polaris namespace")
	// ErrServiceTokenRequired 接入点要求引入已有北极星服务时提供服务 Token
	ErrServiceTokenRequired = errors.New("polarisToken is required when importing existing polaris service")
	// ErrWeightFactorUnsupported 接入点不支持权重因子与动态权重
	ErrWeightFactorUnsupported = errors.New("polaris access point does not support weight factor or dynamic weight")
)

// IsClientRequestError 调用方输入问题（Token、权限、服务不存在、不允许改的字段、接入点不匹配等），handler 映射为 400。
func IsClientRequestError(err error) bool {
	return errors.Is(err, polarisprovider.ErrUnauthorized) ||
		errors.Is(err, polarisprovider.ErrServiceNotFound) ||
		errors.Is(err, ErrOperatorEmpty) ||
		errors.Is(err, ErrNotManaged) ||
		errors.Is(err, ErrInvalidResourceName) ||
		errors.Is(err, ErrAccessPointMismatch) ||
		errors.Is(err, ErrScopeEnvRequired) ||
		errors.Is(err, ErrInvalidNamespace) ||
		errors.Is(err, ErrServiceTokenRequired) ||
		errors.Is(err, ErrWeightFactorUnsupported) ||
		errors.Is(err, accesspoint.ErrClusterNotBound) ||
		errors.Is(err, accesspoint.ErrAccessPointNotFound)
}
