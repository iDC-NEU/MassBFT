package ecs

import (
	"errors"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	"github.com/alibabacloud-go/tea/tea"
)

type InstanceConfig struct {
	RegionId     *string
	ImageId      *string
	SGId         *string
	VSId         *string
	InstanceType *string
	Password     *string
	Amount       int
}

func NewInstanceConfig() *InstanceConfig {
	return &InstanceConfig{
		Password: tea.String("neu1234."),
	}
}

func CreateSpotInstance(client *openapi.Client, config *InstanceConfig) ([]string, error) {
	params := createDefaultParam()
	params.Action = tea.String("RunInstances")

	queries := map[string]interface{}{}
	queries["RegionId"] = config.RegionId
	queries["ImageId"] = config.ImageId
	queries["SecurityGroupId"] = config.SGId
	queries["VSwitchId"] = config.VSId
	queries["InstanceType"] = config.InstanceType
	queries["Password"] = config.Password
	queries["SystemDisk.Category"] = tea.String("cloud_essd")
	queries["Amount"] = config.Amount
	queries["MinAmount"] = config.Amount
	queries["InternetMaxBandwidthOut"] = 10
	queries["InstanceChargeType"] = tea.String("PostPaid")
	queries["SpotStrategy"] = tea.String("SpotAsPriceGo")
	queries["SpotDuration"] = 0
	// queries["DryRun"] = true

	body, err := callAPIWithRuntime(client, params, queries)
	if err != nil {
		return nil, err
	}
	values, ok := body["InstanceIdSets"].(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid values")
	}
	valueList, ok := values["InstanceIdSet"].([]interface{})
	if !ok {
		return nil, errors.New("invalid value list")
	}
	ret := make([]string, 0)
	for _, it := range valueList {
		ret = append(ret, it.(string))
	}
	return ret, nil
}

func CreateSpotInstanceSlow(client *openapi.Client, regionId *string, instanceType *string, amount int, imageName *string) ([]string, error) {
	instanceConfig := NewInstanceConfig()
	instanceConfig.RegionId = regionId

	imageList, err := ListImage(client, regionId, imageName)
	if err != nil || len(imageList) == 0 {
		return nil, err
	}

	instanceConfig.ImageId = tea.String(imageList[0])

	vsList, err := ListVSList(client, regionId, nil)
	if err != nil || len(vsList) == 0 {
		return nil, err
	}
	instanceConfig.VSId = tea.String(vsList[0])

	sgList, err := ListSGList(client, regionId)
	if err != nil || len(sgList) == 0 {
		return nil, err
	}
	instanceConfig.SGId = tea.String(sgList[0])

	instanceConfig.InstanceType = instanceType
	instanceConfig.Amount = amount

	return CreateSpotInstance(client, instanceConfig)
}
