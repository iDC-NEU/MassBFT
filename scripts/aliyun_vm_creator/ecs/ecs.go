package ecs

import (
	"errors"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	openapiutil "github.com/alibabacloud-go/openapi-util/service"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
)

func CreateClient(accessKeyId *string, accessKeySecret *string, regionName *string) (*openapi.Client, error) {
	config := &openapi.Config{
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		Endpoint:        tea.String("ecs." + *regionName + ".aliyuncs.com"),
	}
	return openapi.NewClient(config)
}

func createDefaultParam() *openapi.Params {
	return &openapi.Params{
		Version:     tea.String("2014-05-26"),
		Protocol:    tea.String("HTTPS"),
		Method:      tea.String("POST"),
		AuthType:    tea.String("AK"),
		Style:       tea.String("RPC"),
		Pathname:    tea.String("/"),
		ReqBodyType: tea.String("json"),
		BodyType:    tea.String("json"),
	}
}

func callAPIWithRuntimeAndQueries(client *openapi.Client, params *openapi.Params) (map[string]interface{}, error) {
	return callAPIWithRuntime(client, params, nil)
}

func callAPIWithRuntime(client *openapi.Client, params *openapi.Params, queries map[string]interface{}) (map[string]interface{}, error) {
	runtime := &util.RuntimeOptions{}
	request := &openapi.OpenApiRequest{}

	if queries != nil {
		request.Query = openapiutil.Query(queries)
	}

	// 返回值为 Map 类型，可从 Map 中获得三类数据：响应体 body、响应头 headers、HTTP 返回的状态码 statusCode。
	result, err := client.CallApi(params, request, runtime)
	if err != nil {
		return nil, err
	}
	body, ok := result["body"].(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid body")
	}
	return body, nil
}
