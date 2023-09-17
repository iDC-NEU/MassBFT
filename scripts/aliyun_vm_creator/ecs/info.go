package ecs

import (
	"errors"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	"github.com/alibabacloud-go/tea/tea"
)

func getLabelListInBody(body map[string]interface{}, first string, second string, label string) ([]string, error) {
	values, ok := body[first].(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid " + first)
	}
	valueList, ok := values[second].([]interface{})
	if !ok {
		return nil, errors.New("invalid " + first + " list")
	}
	ret := make([]string, 0)
	for _, it := range valueList {
		value, ok := it.(map[string]interface{})
		if !ok {
			return nil, errors.New("invalid value")
		}
		if result, ok := value[label]; ok {
			ret = append(ret, result.(string))
		}
	}
	return ret, nil
}

func ListRegionList(client *openapi.Client) ([]string, error) {
	params := createDefaultParam()
	params.Action = tea.String("DescribeRegions")
	body, err := callAPIWithRuntimeAndQueries(client, params)
	if err != nil {
		return nil, errors.New("call api failure")
	}
	return getLabelListInBody(body, "Regions", "Region", "RegionId")
}

func ListZoneList(client *openapi.Client, region *string) ([]string, error) {
	params := createDefaultParam()
	params.Action = tea.String("DescribeZones")
	queries := map[string]interface{}{}
	queries["RegionId"] = region
	body, err := callAPIWithRuntime(client, params, queries)
	if err != nil {
		return nil, errors.New("call api failure")
	}
	return getLabelListInBody(body, "Zones", "Zone", "ZoneId")
}

func ListSGList(client *openapi.Client, region *string) ([]string, error) {
	params := createDefaultParam()
	params.Action = tea.String("DescribeSecurityGroups")
	queries := map[string]interface{}{}
	queries["RegionId"] = region
	body, err := callAPIWithRuntime(client, params, queries)
	if err != nil {
		return nil, errors.New("call api failure")
	}
	return getLabelListInBody(body, "SecurityGroups", "SecurityGroup", "SecurityGroupId")
}

func ListVSList(client *openapi.Client, region *string, zone *string) ([]string, error) {
	params := createDefaultParam()
	params.Action = tea.String("DescribeVSwitches")
	queries := map[string]interface{}{}
	queries["RegionId"] = region
	if zone != nil {
		queries["ZoneId"] = zone
	}
	body, err := callAPIWithRuntime(client, params, queries)
	if err != nil {
		return nil, errors.New("call api failure")
	}
	return getLabelListInBody(body, "VSwitches", "VSwitch", "VSwitchId")
}

func ListImage(client *openapi.Client, region *string, name *string) ([]string, error) {
	params := createDefaultParam()
	params.Action = tea.String("DescribeImages")
	queries := map[string]interface{}{}
	queries["RegionId"] = region
	if name != nil {
		queries["ImageName"] = name
	}
	body, err := callAPIWithRuntime(client, params, queries)
	if err != nil {
		return nil, errors.New("call api failure")
	}
	return getLabelListInBody(body, "Images", "Image", "ImageId")
}

type InstanceInfo struct {
	Id    string
	PubIp string
	PriIp string
}

func getPrivateIpFromInstance(instance map[string]interface{}) (string, error) {
	ips, err := getLabelListInBody(instance, "NetworkInterfaces", "NetworkInterface", "PrimaryIpAddress")
	if err != nil || len(ips) == 0 {
		return "", errors.New("invalid private ip")
	}
	return ips[0], nil
}

func getPublicIpFromInstance(instance map[string]interface{}) (string, error) {
	value, ok := instance["PublicIpAddress"].(map[string]interface{})
	if !ok {
		return "", errors.New("invalid instances")
	}
	ip, ok := value["IpAddress"].([]interface{})
	if !ok || len(ip) == 0 {
		return "", errors.New("invalid instances")
	}
	return ip[0].(string), nil
}

func parseInstanceList(body map[string]interface{}) ([]*InstanceInfo, error) {
	values, ok := body["Instances"].(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid instances")
	}
	valueList, ok := values["Instance"].([]interface{})
	if !ok {
		return nil, errors.New("invalid instances list")
	}
	ret := make([]*InstanceInfo, 0)
	for _, it := range valueList {
		values, ok = it.(map[string]interface{})
		if !ok {
			return nil, errors.New("invalid private ip")
		}
		instance := &InstanceInfo{}
		instance.Id = values["InstanceId"].(string)
		var err error
		instance.PriIp, err = getPrivateIpFromInstance(values)
		if err != nil {
			return nil, errors.New("invalid value")
		}
		instance.PubIp, err = getPublicIpFromInstance(values)
		ret = append(ret, instance)
	}
	return ret, nil
}

func ListInstancesById(client *openapi.Client, region *string, ids []string) ([]*InstanceInfo, error) {
	params := createDefaultParam()
	params.Action = tea.String("DescribeInstances")
	queries := map[string]interface{}{}
	queries["RegionId"] = region
	queries["InstanceIds"] = ids
	queries["PageSize"] = 100
	body, err := callAPIWithRuntime(client, params, queries)
	if err != nil {
		return nil, errors.New("call api failure")
	}
	list, err := parseInstanceList(body)
	if err != nil {
		return nil, err
	}
	fList := make([]*InstanceInfo, 0)
	for _, it := range list {
		for _, id := range ids {
			if it.Id == id {
				fList = append(fList, it)
				break
			}
		}
	}
	if len(fList) == 0 {
		return nil, errors.New("f list is empty")
	}
	return fList, nil
}

func ListInstancesByRegion(client *openapi.Client, region *string, zone *string) ([]*InstanceInfo, error) {
	params := createDefaultParam()
	params.Action = tea.String("DescribeInstances")
	queries := map[string]interface{}{}
	queries["RegionId"] = region
	if zone != nil {
		queries["ZoneId"] = zone
	}
	body, err := callAPIWithRuntime(client, params, queries)
	if err != nil {
		return nil, errors.New("call api failure")
	}
	return parseInstanceList(body)
}
