package main

import (
	"encoding/json"
	"fmt"
	"github.com/alibabacloud-go/tea/tea"
	"time"
)
import "aliyunVMCreator/ecs"

func main() {
	imageName := tea.String("init-cn-hongkong")
	regionName := tea.String("cn-hongkong")
	groupId := 0

	//imageName := tea.String("init-us-west-1")
	//regionName := tea.String("us-west-1")
	//groupId := 1

	//imageName := tea.String("init-eu-west-1")
	//regionName := tea.String("eu-west-1")
	//groupId := 2

	amount := 5
	instanceName := tea.String("ecs.c6.2xlarge")
	key := tea.String("--")
	secret := tea.String("--")
	// Note: set bandwidth in queries["InternetMaxBandwidthOut"] = 10

	client, err := ecs.CreateClient(key, secret, regionName)
	if err != nil {
		panic(err)
	}
	instances, err := ecs.CreateSpotInstanceSlow(
		client,
		regionName,
		instanceName,
		amount,
		imageName)

	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second * 10)
		list, err := ecs.ListInstancesById(client, regionName, instances)
		if err != nil {
			continue
		}
		result := make([]interface{}, 0)
		for idx, it := range list {
			data := map[string]interface{}{
				"group_id":  groupId,
				"node_id":   idx,
				"pub":       it.PubIp,
				"pri":       it.PriIp,
				"is_client": false,
			}
			if idx == len(list)-1 {
				data["node_id"] = 0
				data["is_client"] = true
			}
			result = append(result, data)
		}

		jsonData, err := json.Marshal(result)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println(string(jsonData))
		break
	}
}
