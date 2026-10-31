//! Keep the library's authenticated client (including DFS), but use its public
//! connection/tree APIs for a real NTLM anonymous session. smb2 0.31's normal
//! authenticator computes an NTLMv2 response even for an empty user/password;
//! Samba with map-to-guest=Never correctly refuses that as a failed login.
use smb2::{
    client::{connection::Connection, NegotiatedParams},
    ClientConfig, DirectoryReader, FileInfo, FileReader, FileWriter, SmbClient, Tree,
};
use std::sync::Arc;

pub(super) enum Client {
    Named(Box<SmbClient>),
    Anonymous(Anonymous),
}
pub(super) struct Anonymous(Connection);
impl Drop for Anonymous {
    fn drop(&mut self) {
        self.0.mark_dead();
    }
}
impl Client {
    pub(super) async fn connect(config: ClientConfig) -> smb2::Result<Self> {
        if !config.username.is_empty() {
            return SmbClient::connect(config)
                .await
                .map(|client| Self::Named(Box::new(client)));
        }
        if config.dfs_enabled {
            return Err(smb2::Error::invalid_data(
                "DFS is not supported for anonymous sessions",
            ));
        }
        let connection = Connection::connect_with(
            &config.addr,
            smb2::transport::ConnectOptions::with_timeout(config.timeout),
        )
        .await?;
        let mut pending = Anonymous(connection);
        pending.0.set_response_timeout(config.response_timeout);
        pending.0.set_compression_requested(config.compression);
        pending.0.negotiate().await?;
        login_anonymous(&mut pending.0).await?;
        Ok(Self::Anonymous(pending))
    }
    pub(super) fn connection(&self) -> &Connection {
        match self {
            Self::Named(client) => client.connection(),
            Self::Anonymous(client) => &client.0,
        }
    }
    pub(super) fn connection_mut(&mut self) -> &mut Connection {
        match self {
            Self::Named(client) => client.connection_mut(),
            Self::Anonymous(client) => &mut client.0,
        }
    }
    pub(super) fn params(&self) -> Option<NegotiatedParams> {
        self.connection().params()
    }
    pub(super) fn session(&self) -> Option<&smb2::Session> {
        match self {
            Self::Named(client) => Some(client.session()),
            Self::Anonymous(_) => None,
        }
    }
    pub(super) fn is_disconnected(&self) -> bool {
        self.connection().is_disconnected()
    }
    pub(super) async fn connect_share(&mut self, name: &str) -> smb2::Result<Tree> {
        match self {
            Self::Named(client) => client.connect_share(name).await,
            Self::Anonymous(client) => Tree::connect(&mut client.0, name).await,
        }
    }
    pub(super) async fn disconnect_share(&mut self, tree: &Tree) -> smb2::Result<()> {
        match self {
            Self::Named(client) => client.disconnect_share(tree).await,
            Self::Anonymous(client) => tree.disconnect(&mut client.0).await,
        }
    }
    pub(super) async fn list_shares(&mut self) -> smb2::Result<Vec<smb2::ShareInfo>> {
        match self {
            Self::Named(client) => client.list_shares().await,
            Self::Anonymous(client) => smb2::client::list_shares(&mut client.0).await,
        }
    }
    pub(super) async fn stat(&mut self, tree: &mut Tree, path: &str) -> smb2::Result<FileInfo> {
        match self {
            Self::Named(client) => client.stat(tree, path).await,
            Self::Anonymous(client) => tree.stat(&mut client.0, path).await,
        }
    }
    pub(super) async fn open_directory_reader(
        &mut self,
        tree: &mut Tree,
        path: &str,
    ) -> smb2::Result<DirectoryReader> {
        match self {
            Self::Named(client) => client.open_directory_reader(tree, path).await,
            Self::Anonymous(client) => tree.open_directory_reader(client.0.clone(), path).await,
        }
    }
    pub(super) async fn open_file_reader(
        &mut self,
        tree: &Tree,
        path: &str,
    ) -> smb2::Result<FileReader> {
        match self {
            Self::Named(client) => client.open_file_reader(tree, path).await,
            Self::Anonymous(client) => {
                Arc::new(tree.clone())
                    .open_file_reader(client.0.clone(), path)
                    .await
            }
        }
    }
    pub(super) async fn create_file_writer(
        &mut self,
        tree: &Tree,
        path: &str,
    ) -> smb2::Result<FileWriter> {
        match self {
            Self::Named(client) => client.create_file_writer(tree, path).await,
            Self::Anonymous(client) => {
                Arc::new(tree.clone())
                    .create_file_writer(client.0.clone(), path)
                    .await
            }
        }
    }
    pub(super) async fn create_file_writer_exclusive(
        &mut self,
        tree: &Tree,
        path: &str,
    ) -> smb2::Result<FileWriter> {
        match self {
            Self::Named(client) => client.create_file_writer_exclusive(tree, path).await,
            Self::Anonymous(client) => {
                Arc::new(tree.clone())
                    .create_file_writer_exclusive(client.0.clone(), path)
                    .await
            }
        }
    }
    pub(super) async fn remove_file(&mut self, tree: &Tree, path: &str) -> smb2::Result<()> {
        match self {
            Self::Named(client) => client.delete_file(tree, path).await,
            Self::Anonymous(client) => tree.delete_file(&mut client.0, path).await,
        }
    }
    pub(super) async fn rename_file(
        &mut self,
        tree: &Tree,
        from: &str,
        to: &str,
        overwrite: bool,
    ) -> smb2::Result<()> {
        let options = smb2::RenameOptions::new().replace_if_exists(overwrite);
        match self {
            Self::Named(client) => client.rename_with(tree, from, to, options).await,
            Self::Anonymous(client) => tree.rename_with(&mut client.0, from, to, options).await,
        }
    }
    pub(super) async fn create_directory(&mut self, tree: &Tree, path: &str) -> smb2::Result<()> {
        match self {
            Self::Named(client) => client.create_directory(tree, path).await,
            Self::Anonymous(client) => tree.create_directory(&mut client.0, path).await,
        }
    }
}

/// MS-NLMP anonymous AUTHENTICATE_MESSAGE: no user/domain/workstation/key or
/// NT challenge response; LM response is one zero byte, and ANONYMOUS is set.
fn anonymous_authenticate() -> Vec<u8> {
    let mut message = vec![0; 65];
    message[..8].copy_from_slice(b"NTLMSSP\0");
    message[8..12].copy_from_slice(&3u32.to_le_bytes());
    for offset in [12, 20, 28, 36, 44, 52] {
        message[offset + 4..offset + 8].copy_from_slice(&64u32.to_le_bytes());
    }
    message[12..14].copy_from_slice(&1u16.to_le_bytes());
    message[14..16].copy_from_slice(&1u16.to_le_bytes());
    message[60..64].copy_from_slice(&(0x0000_0001u32 | 0x0000_0200 | 0x0000_0800).to_le_bytes());
    message
}
async fn login_anonymous(connection: &mut Connection) -> smb2::Result<()> {
    use smb2::{
        auth::ntlm::{NtlmAuthenticator, NtlmCredentials},
        msg::session_setup::{SessionSetupRequest, SessionSetupRequestFlags, SessionSetupResponse},
        pack::{ReadCursor, Unpack},
        types::{
            flags::{Capabilities, SecurityMode},
            status::NtStatus,
            Command,
        },
    };
    if connection
        .params()
        .is_some_and(|params| params.signing_required)
    {
        return Err(smb2::Error::Auth {
            message: "anonymous sessions cannot satisfy required signing".into(),
        });
    }
    let request = |security_buffer| SessionSetupRequest {
        flags: SessionSetupRequestFlags(0),
        security_mode: SecurityMode::new(SecurityMode::SIGNING_ENABLED),
        capabilities: Capabilities::default(),
        channel: 0,
        previous_session_id: 0,
        security_buffer,
    };
    let mut auth = NtlmAuthenticator::new(NtlmCredentials {
        username: String::new(),
        domain: String::new(),
        password: String::new(),
    });
    let first = connection
        .execute(Command::SessionSetup, &request(auth.negotiate()), None)
        .await?;
    if first.header.command != Command::SessionSetup {
        return Err(smb2::Error::invalid_data(
            "invalid anonymous session response",
        ));
    }
    if !first.header.status.is_more_processing_required() {
        return Err(smb2::Error::Protocol {
            status: first.header.status,
            command: Command::SessionSetup,
        });
    }
    let challenge = SessionSetupResponse::unpack(&mut ReadCursor::new(&first.body))?;
    if challenge.security_buffer.len() < 32
        || &challenge.security_buffer[..8] != b"NTLMSSP\0"
        || challenge.security_buffer[8..12] != 2u32.to_le_bytes()
    {
        return Err(smb2::Error::invalid_data(
            "invalid anonymous NTLM challenge",
        ));
    }
    connection.set_session_id(first.header.session_id);
    let final_frame = connection
        .execute(
            Command::SessionSetup,
            &request(anonymous_authenticate()),
            None,
        )
        .await?;
    if final_frame.header.command != Command::SessionSetup {
        return Err(smb2::Error::invalid_data(
            "invalid anonymous session response",
        ));
    }
    if final_frame.header.status != NtStatus::SUCCESS {
        return Err(smb2::Error::Protocol {
            status: final_frame.header.status,
            command: Command::SessionSetup,
        });
    }
    let response = SessionSetupResponse::unpack(&mut ReadCursor::new(&final_frame.body))?;
    // Samba can return flags=0 even for a successful explicit NTLM anonymous
    // exchange. We sent no identity/proof, so there is no authenticated identity
    // to downgrade. Encryption cannot work without a session key, however.
    if response.session_flags.encrypt_data() {
        return Err(smb2::Error::Auth {
            message: "server did not establish an unsigned anonymous session".into(),
        });
    }
    let session = smb2::Session {
        session_id: final_frame.header.session_id,
        signing_key: Vec::new(),
        encryption_key: None,
        decryption_key: None,
        signing_algorithm: smb2::crypto::signing::SigningAlgorithm::HmacSha256,
        should_sign: false,
        should_encrypt: false,
    };
    connection.set_session_id(session.session_id);
    connection.adopt_session(&session);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn anonymous_message_contains_no_ntlm_password_proof_or_identity() {
        let message = anonymous_authenticate();
        assert_eq!(&message[..8], b"NTLMSSP\0");
        assert_eq!(u16::from_le_bytes(message[12..14].try_into().unwrap()), 1);
        for offset in [20, 28, 36, 44, 52] {
            assert_eq!(
                u16::from_le_bytes(message[offset..offset + 2].try_into().unwrap()),
                0
            );
        }
        assert_ne!(
            u32::from_le_bytes(message[60..64].try_into().unwrap()) & 0x800,
            0
        );
        assert_eq!(message[64], 0);
    }
}
