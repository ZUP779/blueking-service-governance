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

package httpcli

import (
	"net"
	"net/http"
	"syscall"

	"github.com/pkg/errors"
)

// ErrForbiddenAddress 目标地址是不允许访问的地址，见 IsForbiddenIP
var ErrForbiddenAddress = errors.New("loopback, link-local and unspecified addresses are not allowed")

// NewUntrustedClient 访问用户填写地址（如自建北极星）的 HTTP 客户端，超时由调用方的 ctx 或 Client.Timeout 控制。
//
// 不跟随重定向：重定向可以把请求引到任意主机、端口与路径，Go 还会把鉴权请求头带到重定向后的主机；
// 收到 3xx 时按非预期响应处理。
// 建立连接前按解析后的 IP 拒绝回环、链路本地（含云元数据地址）等地址，域名解析到这些地址同样拒绝；
// 不拒绝内网私有地址，用户自建的服务通常就部署在内网
func NewUntrustedClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// 必须直连：经过代理时，CheckDialAddress 校验的是代理的地址
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Control: CheckDialAddress}).DialContext
	return &http.Client{
		Transport: NewTransportWith(transport),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// CheckDialAddress 在建立连接前检查解析后的目标 IP，address 形如 ip:port
func CheckDialAddress(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return errors.Wrapf(err, "parse dial address %q", address)
	}
	if ip := net.ParseIP(host); ip == nil || IsForbiddenIP(ip) {
		return ErrForbiddenAddress
	}
	return nil
}

// IsForbiddenIP 不允许访问的地址：回环、链路本地（169.254.0.0/16 含云元数据地址、fe80::/10）、未指定地址与组播地址
func IsForbiddenIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast()
}
