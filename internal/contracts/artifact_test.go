package contracts

import "testing"

func TestArtifactKindMatchesPythonSchemaCasing(t *testing.T) {
	cases := []struct {
		got  ArtifactKind
		want string
	}{
		{ArtifactKindIntermediate, "INTERMEDIATE"},
		{ArtifactKindFinal, "FINAL"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Fatalf("ArtifactKind %q does not match Python schema string %q", tc.got, tc.want)
		}
	}
}
