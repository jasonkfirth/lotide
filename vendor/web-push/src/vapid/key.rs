use p256::{ecdsa::SigningKey, pkcs8::DecodePrivateKey, SecretKey};

/// The P256 curve key pair used for VAPID ECDHSA.
pub struct VapidKey(pub SigningKey);

impl Clone for VapidKey {
    fn clone(&self) -> Self {
        VapidKey(self.0.clone())
    }
}

impl VapidKey {
    /// Gets the uncompressed public key bytes derived from this private key.
    pub fn public_key(&self) -> Vec<u8> {
        self.0.verifying_key().to_encoded_point(false).as_bytes().to_vec()
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, p256::ecdsa::Error> {
        SigningKey::from_slice(bytes).map(VapidKey)
    }

    pub(crate) fn from_pkcs8_pem(pem: &str) -> Result<Self, p256::pkcs8::Error> {
        SecretKey::from_pkcs8_pem(pem).map(|secret_key| VapidKey(secret_key.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    #[test]
    /// Tests that VapidKey derives the correct public key.
    fn test_public_key_derivation() {
        let f = File::open("resources/vapid_test_key.pem").unwrap();
        let key = crate::VapidSignatureBuilder::read_pem(f).unwrap();

        assert_eq!(
            vec![
                4, 202, 53, 30, 162, 133, 234, 201, 12, 101, 140, 164, 174, 215, 189, 118, 234, 152, 192, 16, 244, 242,
                96, 208, 41, 59, 167, 70, 66, 93, 15, 123, 19, 39, 209, 62, 203, 35, 122, 176, 153, 79, 89, 58, 74, 54,
                26, 126, 203, 98, 158, 75, 170, 0, 52, 113, 126, 171, 124, 55, 237, 176, 165, 111, 181
            ],
            key.public_key()
        );
    }

    #[test]
    /// Tests that VapidKey clones properly.
    fn test_key_clones() {
        let f = File::open("resources/vapid_test_key.pem").unwrap();
        let key = crate::VapidSignatureBuilder::read_pem(f).unwrap();

        let key2 = key.clone();

        assert_eq!(key.public_key(), key2.public_key())
    }
}
