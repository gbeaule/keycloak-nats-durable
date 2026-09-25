# Complete Keycloak event catalogue

This catalogue is extracted from the Keycloak 26.7.4 API enums and checked against that compile baseline in Maven tests. Every row uses one of the two [documented data shapes](events.md); omitted optional fields do not create another schema. Deprecated and rarely emitted types are included. Runtime releases can emit different subsets; this list is not a promise that every operation emits an event.

Sources: [EventType](https://github.com/keycloak/keycloak/blob/26.7.4/server-spi-private/src/main/java/org/keycloak/events/EventType.java), [ResourceType](https://github.com/keycloak/keycloak/blob/26.7.4/server-spi-private/src/main/java/org/keycloak/events/admin/ResourceType.java), [OperationType](https://github.com/keycloak/keycloak/blob/26.7.4/server-spi-private/src/main/java/org/keycloak/events/admin/OperationType.java).

## User events

| `data.eventType` | Subject suffix / envelope type suffix |
|---|---|
| `LOGIN` | `user.login` |
| `LOGIN_ERROR` | `user.login_error` |
| `REGISTER` | `user.register` |
| `REGISTER_ERROR` | `user.register_error` |
| `LOGOUT` | `user.logout` |
| `LOGOUT_ERROR` | `user.logout_error` |
| `CODE_TO_TOKEN` | `user.code_to_token` |
| `CODE_TO_TOKEN_ERROR` | `user.code_to_token_error` |
| `CLIENT_LOGIN` | `user.client_login` |
| `CLIENT_LOGIN_ERROR` | `user.client_login_error` |
| `REFRESH_TOKEN` | `user.refresh_token` |
| `REFRESH_TOKEN_ERROR` | `user.refresh_token_error` |
| `VALIDATE_ACCESS_TOKEN` | `user.validate_access_token` |
| `VALIDATE_ACCESS_TOKEN_ERROR` | `user.validate_access_token_error` |
| `INTROSPECT_TOKEN` | `user.introspect_token` |
| `INTROSPECT_TOKEN_ERROR` | `user.introspect_token_error` |
| `FEDERATED_IDENTITY_LINK` | `user.federated_identity_link` |
| `FEDERATED_IDENTITY_LINK_ERROR` | `user.federated_identity_link_error` |
| `REMOVE_FEDERATED_IDENTITY` | `user.remove_federated_identity` |
| `REMOVE_FEDERATED_IDENTITY_ERROR` | `user.remove_federated_identity_error` |
| `UPDATE_EMAIL` | `user.update_email` |
| `UPDATE_EMAIL_ERROR` | `user.update_email_error` |
| `UPDATE_PROFILE` | `user.update_profile` |
| `UPDATE_PROFILE_ERROR` | `user.update_profile_error` |
| `UPDATE_PASSWORD` | `user.update_password` |
| `UPDATE_PASSWORD_ERROR` | `user.update_password_error` |
| `UPDATE_TOTP` | `user.update_totp` |
| `UPDATE_TOTP_ERROR` | `user.update_totp_error` |
| `VERIFY_EMAIL` | `user.verify_email` |
| `VERIFY_EMAIL_ERROR` | `user.verify_email_error` |
| `VERIFY_PROFILE` | `user.verify_profile` |
| `VERIFY_PROFILE_ERROR` | `user.verify_profile_error` |
| `REMOVE_TOTP` | `user.remove_totp` |
| `REMOVE_TOTP_ERROR` | `user.remove_totp_error` |
| `GRANT_CONSENT` | `user.grant_consent` |
| `GRANT_CONSENT_ERROR` | `user.grant_consent_error` |
| `UPDATE_CONSENT` | `user.update_consent` |
| `UPDATE_CONSENT_ERROR` | `user.update_consent_error` |
| `REVOKE_GRANT` | `user.revoke_grant` |
| `REVOKE_GRANT_ERROR` | `user.revoke_grant_error` |
| `SEND_VERIFY_EMAIL` | `user.send_verify_email` |
| `SEND_VERIFY_EMAIL_ERROR` | `user.send_verify_email_error` |
| `SEND_RESET_PASSWORD` | `user.send_reset_password` |
| `SEND_RESET_PASSWORD_ERROR` | `user.send_reset_password_error` |
| `SEND_IDENTITY_PROVIDER_LINK` | `user.send_identity_provider_link` |
| `SEND_IDENTITY_PROVIDER_LINK_ERROR` | `user.send_identity_provider_link_error` |
| `RESET_PASSWORD` | `user.reset_password` |
| `RESET_PASSWORD_ERROR` | `user.reset_password_error` |
| `RESTART_AUTHENTICATION` | `user.restart_authentication` |
| `RESTART_AUTHENTICATION_ERROR` | `user.restart_authentication_error` |
| `INVALID_SIGNATURE` | `user.invalid_signature` |
| `INVALID_SIGNATURE_ERROR` | `user.invalid_signature_error` |
| `REGISTER_NODE` | `user.register_node` |
| `REGISTER_NODE_ERROR` | `user.register_node_error` |
| `UNREGISTER_NODE` | `user.unregister_node` |
| `UNREGISTER_NODE_ERROR` | `user.unregister_node_error` |
| `USER_INFO_REQUEST` | `user.user_info_request` |
| `USER_INFO_REQUEST_ERROR` | `user.user_info_request_error` |
| `IDENTITY_PROVIDER_LINK_ACCOUNT` | `user.identity_provider_link_account` |
| `IDENTITY_PROVIDER_LINK_ACCOUNT_ERROR` | `user.identity_provider_link_account_error` |
| `IDENTITY_PROVIDER_LOGIN` | `user.identity_provider_login` |
| `IDENTITY_PROVIDER_LOGIN_ERROR` | `user.identity_provider_login_error` |
| `IDENTITY_PROVIDER_FIRST_LOGIN` | `user.identity_provider_first_login` |
| `IDENTITY_PROVIDER_FIRST_LOGIN_ERROR` | `user.identity_provider_first_login_error` |
| `IDENTITY_PROVIDER_POST_LOGIN` | `user.identity_provider_post_login` |
| `IDENTITY_PROVIDER_POST_LOGIN_ERROR` | `user.identity_provider_post_login_error` |
| `IDENTITY_PROVIDER_RESPONSE` | `user.identity_provider_response` |
| `IDENTITY_PROVIDER_RESPONSE_ERROR` | `user.identity_provider_response_error` |
| `IDENTITY_PROVIDER_RETRIEVE_TOKEN` | `user.identity_provider_retrieve_token` |
| `IDENTITY_PROVIDER_RETRIEVE_TOKEN_ERROR` | `user.identity_provider_retrieve_token_error` |
| `IMPERSONATE` | `user.impersonate` |
| `IMPERSONATE_ERROR` | `user.impersonate_error` |
| `CUSTOM_REQUIRED_ACTION` | `user.custom_required_action` |
| `CUSTOM_REQUIRED_ACTION_ERROR` | `user.custom_required_action_error` |
| `EXECUTE_ACTIONS` | `user.execute_actions` |
| `EXECUTE_ACTIONS_ERROR` | `user.execute_actions_error` |
| `EXECUTE_ACTION_TOKEN` | `user.execute_action_token` |
| `EXECUTE_ACTION_TOKEN_ERROR` | `user.execute_action_token_error` |
| `CLIENT_INFO` | `user.client_info` |
| `CLIENT_INFO_ERROR` | `user.client_info_error` |
| `CLIENT_REGISTER` | `user.client_register` |
| `CLIENT_REGISTER_ERROR` | `user.client_register_error` |
| `CLIENT_UPDATE` | `user.client_update` |
| `CLIENT_UPDATE_ERROR` | `user.client_update_error` |
| `CLIENT_DELETE` | `user.client_delete` |
| `CLIENT_DELETE_ERROR` | `user.client_delete_error` |
| `CLIENT_INITIATED_ACCOUNT_LINKING` | `user.client_initiated_account_linking` |
| `CLIENT_INITIATED_ACCOUNT_LINKING_ERROR` | `user.client_initiated_account_linking_error` |
| `TOKEN_EXCHANGE` | `user.token_exchange` |
| `TOKEN_EXCHANGE_ERROR` | `user.token_exchange_error` |
| `OAUTH2_DEVICE_AUTH` | `user.oauth2_device_auth` |
| `OAUTH2_DEVICE_AUTH_ERROR` | `user.oauth2_device_auth_error` |
| `OAUTH2_DEVICE_VERIFY_USER_CODE` | `user.oauth2_device_verify_user_code` |
| `OAUTH2_DEVICE_VERIFY_USER_CODE_ERROR` | `user.oauth2_device_verify_user_code_error` |
| `OAUTH2_DEVICE_CODE_TO_TOKEN` | `user.oauth2_device_code_to_token` |
| `OAUTH2_DEVICE_CODE_TO_TOKEN_ERROR` | `user.oauth2_device_code_to_token_error` |
| `AUTHREQID_TO_TOKEN` | `user.authreqid_to_token` |
| `AUTHREQID_TO_TOKEN_ERROR` | `user.authreqid_to_token_error` |
| `PERMISSION_TOKEN` | `user.permission_token` |
| `PERMISSION_TOKEN_ERROR` | `user.permission_token_error` |
| `DELETE_ACCOUNT` | `user.delete_account` |
| `DELETE_ACCOUNT_ERROR` | `user.delete_account_error` |
| `PUSHED_AUTHORIZATION_REQUEST` | `user.pushed_authorization_request` |
| `PUSHED_AUTHORIZATION_REQUEST_ERROR` | `user.pushed_authorization_request_error` |
| `USER_DISABLED_BY_PERMANENT_LOCKOUT` | `user.user_disabled_by_permanent_lockout` |
| `USER_DISABLED_BY_PERMANENT_LOCKOUT_ERROR` | `user.user_disabled_by_permanent_lockout_error` |
| `USER_DISABLED_BY_TEMPORARY_LOCKOUT` | `user.user_disabled_by_temporary_lockout` |
| `USER_DISABLED_BY_TEMPORARY_LOCKOUT_ERROR` | `user.user_disabled_by_temporary_lockout_error` |
| `OAUTH2_EXTENSION_GRANT` | `user.oauth2_extension_grant` |
| `OAUTH2_EXTENSION_GRANT_ERROR` | `user.oauth2_extension_grant_error` |
| `FEDERATED_IDENTITY_OVERRIDE_LINK` | `user.federated_identity_override_link` |
| `FEDERATED_IDENTITY_OVERRIDE_LINK_ERROR` | `user.federated_identity_override_link_error` |
| `UPDATE_CREDENTIAL` | `user.update_credential` |
| `UPDATE_CREDENTIAL_ERROR` | `user.update_credential_error` |
| `REMOVE_CREDENTIAL` | `user.remove_credential` |
| `REMOVE_CREDENTIAL_ERROR` | `user.remove_credential_error` |
| `INVITE_ORG` | `user.invite_org` |
| `INVITE_ORG_ERROR` | `user.invite_org_error` |
| `USER_SESSION_DELETED` | `user.user_session_deleted` |
| `USER_SESSION_DELETED_ERROR` | `user.user_session_deleted_error` |
| `VERIFIABLE_CREDENTIAL_REQUEST` | `user.verifiable_credential_request` |
| `VERIFIABLE_CREDENTIAL_REQUEST_ERROR` | `user.verifiable_credential_request_error` |
| `VERIFIABLE_CREDENTIAL_OFFER_REQUEST` | `user.verifiable_credential_offer_request` |
| `VERIFIABLE_CREDENTIAL_OFFER_REQUEST_ERROR` | `user.verifiable_credential_offer_request_error` |
| `VERIFIABLE_CREDENTIAL_NONCE_REQUEST` | `user.verifiable_credential_nonce_request` |
| `VERIFIABLE_CREDENTIAL_NONCE_REQUEST_ERROR` | `user.verifiable_credential_nonce_request_error` |
| `VERIFIABLE_CREDENTIAL_CREATE_OFFER` | `user.verifiable_credential_create_offer` |
| `VERIFIABLE_CREDENTIAL_CREATE_OFFER_ERROR` | `user.verifiable_credential_create_offer_error` |
| `VERIFIABLE_CREDENTIAL_PRE_AUTHORIZED_GRANT` | `user.verifiable_credential_pre_authorized_grant` |
| `VERIFIABLE_CREDENTIAL_PRE_AUTHORIZED_GRANT_ERROR` | `user.verifiable_credential_pre_authorized_grant_error` |
| `JWT_AUTHORIZATION_GRANT` | `user.jwt_authorization_grant` |
| `JWT_AUTHORIZATION_GRANT_ERROR` | `user.jwt_authorization_grant_error` |

## Admin operations

Any emitted admin event uses the admin shape. The resource and operation are separate dimensions; not every combination is emitted by Keycloak.

| `data.operationType` | Envelope type | Subject ending |
|---|---|---|
| `CREATE` | `io.keycloak.admin.create` | `admin.<resourceToken>.create` |
| `UPDATE` | `io.keycloak.admin.update` | `admin.<resourceToken>.update` |
| `DELETE` | `io.keycloak.admin.delete` | `admin.<resourceToken>.delete` |
| `ACTION` | `io.keycloak.admin.action` | `admin.<resourceToken>.action` |

## Built-in admin resources

| `data.resourceType` | Subject resource token |
|---|---|
| `REALM` | `realm` |
| `REALM_ROLE` | `realm_role` |
| `REALM_ROLE_MAPPING` | `realm_role_mapping` |
| `REALM_SCOPE_MAPPING` | `realm_scope_mapping` |
| `AUTH_FLOW` | `auth_flow` |
| `AUTH_EXECUTION_FLOW` | `auth_execution_flow` |
| `AUTH_EXECUTION` | `auth_execution` |
| `AUTHENTICATOR_CONFIG` | `authenticator_config` |
| `REQUIRED_ACTION_CONFIG` | `required_action_config` |
| `REQUIRED_ACTION` | `required_action` |
| `IDENTITY_PROVIDER` | `identity_provider` |
| `IDENTITY_PROVIDER_MAPPER` | `identity_provider_mapper` |
| `PROTOCOL_MAPPER` | `protocol_mapper` |
| `USER` | `user` |
| `USER_LOGIN_FAILURE` | `user_login_failure` |
| `USER_SESSION` | `user_session` |
| `USER_FEDERATION_PROVIDER` | `user_federation_provider` |
| `USER_FEDERATION_MAPPER` | `user_federation_mapper` |
| `GROUP` | `group` |
| `GROUP_MEMBERSHIP` | `group_membership` |
| `CLIENT` | `client` |
| `CLIENT_INITIAL_ACCESS_MODEL` | `client_initial_access_model` |
| `CLIENT_ROLE` | `client_role` |
| `CLIENT_ROLE_MAPPING` | `client_role_mapping` |
| `CLIENT_SCOPE` | `client_scope` |
| `CLIENT_SCOPE_MAPPING` | `client_scope_mapping` |
| `CLIENT_SCOPE_CLIENT_MAPPING` | `client_scope_client_mapping` |
| `CLUSTER_NODE` | `cluster_node` |
| `COMPONENT` | `component` |
| `AUTHORIZATION_RESOURCE_SERVER` | `authorization_resource_server` |
| `AUTHORIZATION_RESOURCE` | `authorization_resource` |
| `AUTHORIZATION_SCOPE` | `authorization_scope` |
| `AUTHORIZATION_POLICY` | `authorization_policy` |
| `CUSTOM` | `custom` |
| `USER_PROFILE` | `user_profile` |
| `ORGANIZATION` | `organization` |
| `ORGANIZATION_MEMBERSHIP` | `organization_membership` |
| `ORGANIZATION_GROUP` | `organization_group` |
| `ORGANIZATION_GROUP_MEMBERSHIP` | `organization_group_membership` |

Custom resource strings are also supported, using `custom-<base64url(UTF-8 name)>`. Missing resource names use `unknown`. The payload retains the exact supplied name when present.
