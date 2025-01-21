package types

//
//// UpdateCollectionStateRequest is an sdk type which is mapped from the proto.UpdateCollectionStateRequest
//type UpdateCollectionStateRequest struct {
//	TableName     string
//	PartitionName string
//
//
//	// the folder containing collection state files (e.g. last collection time)
//	CollectionStateDir string
//	// the source to use (with raw config)
//	SourceData *SourceConfigData
//
//	// the updateCollectionStateion start time
//	From time.Time
//
//}
//
//func UpdateCollectionStateRequestFromProto(pr *proto.UpdateCollectionStateRequest) (*UpdateCollectionStateRequest, error) {
//	if pr.SourceData == nil {
//		return nil, fmt.Errorf("source data is required")
//	}
//	sourceData, err := ConfigDataFromProto[*SourceConfigData](pr.SourceData)
//	if err != nil {
//		return nil, err
//	}
//
//	// NOTE: add the (possibly nil) SourcePluginReattach to the source data
//	if pr.SourcePlugin != nil {
//		sourceData.SetReattach(pr.SourcePlugin)
//	}
//
//	res := &UpdateCollectionStateRequest{
//		TableName:          pr.TableName,
//		PartitionName:      pr.PartitionName,
//		CollectionStateDir: pr.CollectionStateDir,
//		SourceData:         sourceData,
//		From: pr.FromTime.AsTime(),
//	}
//	return res, nil
//}
