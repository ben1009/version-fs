//! Shared bounded fixture support. Enable with the `test-support` feature.

use crate::format::{FormatError, ObjectType, encode_object};

/// Maximum number of object fixtures accepted by the default test budget.
pub const DEFAULT_MAX_FIXTURE_OBJECTS: usize = 64;
/// Maximum total encoded fixture bytes accepted by the default test budget.
pub const DEFAULT_MAX_FIXTURE_BYTES: usize = 16 * 1024 * 1024;

/// A bounded collection of canonical objects for shared integration fixtures.
#[derive(Clone, Debug)]
pub struct BoundedFixtures {
    max_objects: usize,
    max_bytes: usize,
    encoded_bytes: usize,
    objects: Vec<(ObjectType, Vec<u8>)>,
}

impl Default for BoundedFixtures {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FIXTURE_OBJECTS, DEFAULT_MAX_FIXTURE_BYTES)
    }
}

impl BoundedFixtures {
    /// Creates a fixture set with explicit object-count and encoded-byte limits.
    pub fn new(max_objects: usize, max_bytes: usize) -> Self {
        Self {
            max_objects,
            max_bytes,
            encoded_bytes: 0,
            objects: Vec::new(),
        }
    }

    /// Adds one canonical fixture object if both configured limits permit it.
    pub fn push(&mut self, object_type: ObjectType, payload: &[u8]) -> Result<(), FixtureError> {
        if self.objects.len() >= self.max_objects {
            return Err(FixtureError::ObjectLimit {
                limit: self.max_objects,
            });
        }
        let encoded = encode_object(object_type, payload).map_err(FixtureError::Format)?;
        let total =
            self.encoded_bytes
                .checked_add(encoded.len())
                .ok_or(FixtureError::ByteLimit {
                    limit: self.max_bytes,
                })?;
        if total > self.max_bytes {
            return Err(FixtureError::ByteLimit {
                limit: self.max_bytes,
            });
        }
        self.encoded_bytes = total;
        self.objects.push((object_type, encoded));
        Ok(())
    }

    /// Returns immutable fixture objects in insertion order.
    pub fn objects(&self) -> &[(ObjectType, Vec<u8>)] {
        &self.objects
    }

    /// Returns the total encoded bytes retained by this fixture set.
    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
}

/// Error raised when a bounded fixture budget is exceeded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FixtureError {
    /// Too many fixture objects were added.
    ObjectLimit {
        /// Maximum number of objects in this fixture set.
        limit: usize,
    },
    /// The fixture byte budget was exceeded.
    ByteLimit {
        /// Maximum encoded bytes in this fixture set.
        limit: usize,
    },
    /// The object could not be canonically encoded.
    Format(FormatError),
}
