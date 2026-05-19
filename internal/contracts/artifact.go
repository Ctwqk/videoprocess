package contracts

type ArtifactKind string

const (
	ArtifactKindIntermediate ArtifactKind = "intermediate"
	ArtifactKindFinal        ArtifactKind = "final"
)
