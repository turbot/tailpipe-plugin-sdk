package source

type SourceObserver interface {
	NotifyDiscoverArtifactsStart()
	NotifyDiscoverArtifactsProgress(current int64, total int64)
	NotifyDiscoverArtifactsEnd(err error)
	NotifyArtifactDiscovered(ai *ArtifactInfo)
	NotifyDownloadArtifactStart(ai *ArtifactInfo)
	NotifyDownloadArtifactProgress(ai *ArtifactInfo, current int64, total int64)
	NotifyDownloadArtifactEnd(ai *ArtifactInfo, err error)
	NotifyArtifactDownloaded(a *Artifact)
}
