//! Typed identities used at Version FS protocol boundaries.

use std::fmt;

macro_rules! fixed_id {
    ($name:ident, $bytes:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; $bytes]);

        impl $name {
            /// Creates an identity from its fixed-width byte representation.
            pub const fn from_bytes(bytes: [u8; $bytes]) -> Self {
                Self(bytes)
            }

            /// Returns the fixed-width byte representation.
            pub const fn as_bytes(&self) -> &[u8; $bytes] {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in self.0 {
                    write!(formatter, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    };
}

fixed_id!(
    ObjectId,
    32,
    "A SHA-256 content identity for one encoded Version FS object."
);
fixed_id!(
    WorkspaceId,
    16,
    "An opaque identity for one Version FS workspace."
);
fixed_id!(
    BranchId,
    16,
    "An opaque identity for one Version FS branch."
);
fixed_id!(
    GenerationId,
    16,
    "An opaque identity for one immutable Generation."
);
fixed_id!(
    HistoryId,
    16,
    "A stable identity for one coordinated version history."
);
fixed_id!(
    TimestampDomainId,
    16,
    "An identity for one global timestamp domain."
);
fixed_id!(LogId, 16, "An opaque identity for one native ordered log.");
fixed_id!(
    ArtifactId,
    16,
    "An opaque identity for one named artifact lineage."
);
fixed_id!(
    VolumeBoundaryId,
    16,
    "An identity for one established volume capture boundary."
);
