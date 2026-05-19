package contracts

// ArtifactKind mirrors backend/app/models/artifact.py ArtifactKind.
// The Python enum stores uppercase strings via SQLAlchemy `Enum(ArtifactKind,
// name="artifact_kind", create_constraint=True)`; the database constraint
// rejects lower-case values, so Go writers MUST use the uppercase form.
type ArtifactKind string

const (
	ArtifactKindIntermediate ArtifactKind = "INTERMEDIATE"
	ArtifactKindFinal        ArtifactKind = "FINAL"
)
