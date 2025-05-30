# V1 Architecture, Design and Threat Modeling

## V1.1 Secure Software Development Lifecycle

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.1.1** | Verify the use of a secure software development lifecycle that addresses security in all stages of development. ([C1](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.1.2** | Verify the use of threat modeling for every design change or sprint planning to identify threats, plan for countermeasures, facilitate appropriate risk responses, and guide security testing. | | | |
| **1.1.3** | Verify that all user stories and features contain functional security constraints, such as "As a user, I should be able to view and edit my profile. I should not be able to view or edit anyone else's profile" | | | |
| **1.1.4** | Verify documentation and justification of all the application's trust boundaries, components, and significant data flows. | | | |
| **1.1.5** | Verify definition and security analysis of the application's high-level architecture and all connected remote services. ([C1](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.1.6** | Verify implementation of centralized, simple (economy of design), vetted, secure, and reusable security controls to avoid duplicate, missing, ineffective, or insecure controls. ([C10](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.1.7** | Verify availability of a secure coding checklist, security requirements, guideline, or policy to all developers and testers. | | | |

## V1.2 Authentication Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.2.1** | Verify the use of unique or special low-privilege operating system accounts for all application components, services, and servers. ([C3](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.2.2** | Verify that communications between application components, including APIs, middleware and data layers, are authenticated. Components should have the least necessary privileges needed. ([C3](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.2.3** | Verify that the application uses a single vetted authentication mechanism that is known to be secure, can be extended to include strong authentication, and has sufficient logging and monitoring to detect account abuse or breaches. | | | |
| **1.2.4** | Verify that all authentication pathways and identity management APIs implement consistent authentication security control strength, such that there are no weaker alternatives per the risk of the application. | | | |

## V1.3 Session Management Architecture

This is a placeholder for future architectural requirements.

## V1.4 Access Control Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.4.1** | Verify that trusted enforcement points, such as access control gateways, servers, and serverless functions, enforce access controls. Never enforce access controls on the client. | | | |
| **1.4.2** | [DELETED, NOT ACTIONABLE] | | | |
| **1.4.3** | [DELETED, DUPLICATE OF 4.1.3] | | | |
| **1.4.4** | Verify the application uses a single and well-vetted access control mechanism for accessing protected data and resources. All requests must pass through this single mechanism to avoid copy and paste or insecure alternative paths. ([C7](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.4.5** | Verify that attribute or feature-based access control is used whereby the code checks the user's authorization for a feature/data item rather than just their role. Permissions should still be allocated using roles. ([C7](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |

## V1.5 Input and Output Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.5.1** | Verify that input and output requirements clearly define how to handle and process data based on type, content, and applicable laws, regulations, and other policy compliance. | | | |
| **1.5.2** | Verify that serialization is not used when communicating with untrusted clients. If this is not possible, ensure that adequate integrity controls (and possibly encryption if sensitive data is sent) are enforced to prevent deserialization attacks including object injection. | | | |
| **1.5.3** | Verify that input validation is enforced on a trusted service layer. ([C5](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.5.4** | Verify that output encoding occurs close to or by the interpreter for which it is intended. ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |

## V1.6 Cryptographic Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.6.1** | Verify that there is an explicit policy for management of cryptographic keys and that a cryptographic key lifecycle follows a key management standard such as NIST SP 800-57. | | | |
| **1.6.2** | Verify that consumers of cryptographic services protect key material and other secrets by using key vaults or API based alternatives. | | | |
| **1.6.3** | Verify that all keys and passwords are replaceable and are part of a well-defined process to re-encrypt sensitive data. | | | |
| **1.6.4** | Verify that the architecture treats client-side secrets--such as symmetric keys, passwords, or API tokens--as insecure and never uses them to protect or access sensitive data. | | | |

## V1.7 Errors, Logging and Auditing Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.7.1** | Verify that a common logging format and approach is used across the system. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.7.2** | Verify that logs are securely transmitted to a preferably remote system for analysis, detection, alerting, and escalation. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |

## V1.8 Data Protection and Privacy Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.8.1** | Verify that all sensitive data is identified and classified into protection levels. | | | |
| **1.8.2** | Verify that all protection levels have an associated set of protection requirements, such as encryption requirements, integrity requirements, retention, privacy and other confidentiality requirements, and that these are applied in the architecture. | | | |

## V1.9 Communications Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.9.1** | Verify the application encrypts communications between components, particularly when these components are in different containers, systems, sites, or cloud providers. ([C3](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.9.2** | Verify that application components verify the authenticity of each side in a communication link to prevent person-in-the-middle attacks. For example, application components should validate TLS certificates and chains. | | | |

## V1.10 Malicious Software Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.10.1** | Verify that a source code control system is in use, with procedures to ensure that check-ins are accompanied by issues or change tickets. The source code control system should have access control and identifiable users to allow traceability of any changes. | | | |

## V1.11 Business Logic Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.11.1** | Verify the definition and documentation of all application components in terms of the business or security functions they provide. | | | |
| **1.11.2** | Verify that all high-value business logic flows, including authentication, session management and access control, do not share unsynchronized state. | | | |
| **1.11.3** | _(not applicable to KNIME)_ | | | ✅ |

## V1.12 Secure File Upload Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.12.1** | [DELETED, DUPLICATE OF 12.4.1] | | | |
| **1.12.2** | Verify that user-uploaded files - if required to be displayed or downloaded from the application - are served by either octet stream downloads, or from an unrelated domain, such as a cloud file storage bucket. Implement a suitable Content Security Policy (CSP) to reduce the risk from XSS vectors or other attacks from the uploaded file. | | | |

## V1.13 API Architecture

This is a placeholder for future architectural requirements.

## V1.14 Configuration Architecture

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **1.14.1** | Verify the segregation of components of differing trust levels through well-defined security controls, firewall rules, API gateways, reverse proxies, cloud-based security groups, or similar mechanisms. | | | |
| **1.14.2** | Verify that binary signatures, trusted connections, and verified endpoints are used to deploy binaries to remote devices. | | | |
| **1.14.3** | Verify that the build pipeline warns of out-of-date or insecure components and takes appropriate actions. | | | |
| **1.14.4** | Verify that the build pipeline contains a build step to automatically build and verify the secure deployment of the application, particularly if the application infrastructure is software defined, such as cloud environment build scripts. | | | |
| **1.14.5** | Verify that application deployments adequately sandbox, containerize and/or isolate at the network level to delay and deter attackers from attacking other applications, especially when they are performing sensitive or dangerous actions such as deserialization. ([C5](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **1.14.6** | Verify the application does not use unsupported, insecure, or deprecated client-side technologies such as NSAPI plugins, Flash, Shockwave, ActiveX, Silverlight, NACL, or client-side Java applets. | | | |

# V2 Authentication

## V2.1 Password Security

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.1.1** | Verify that user set passwords are at least 12 characters in length (after multiple spaces are combined). ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no password input | 2025-03-26 | ✅ |
| **2.1.2** | Verify that passwords of at least 64 characters are permitted, and that passwords of more than 128 characters are denied. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no password input | 2025-03-26 | ✅ |
| **2.1.3** | Verify that password truncation is not performed. However, consecutive multiple spaces may be replaced by a single space. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no password input | 2025-03-26 | ✅ |
| **2.1.4** | Verify that any printable Unicode character, including language neutral characters such as spaces and Emojis are permitted in passwords. | There's no password input | 2025-03-26 | ✅ |
| **2.1.5** | Verify users can change their password. | There's no password input | 2025-03-26 | ✅ |
| **2.1.6** | _(not applicable to KNIME)_ | | | ✅ |
| **2.1.7** | Verify that passwords submitted during account registration, login, and password change are checked against a set of breached passwords either locally (such as the top 1,000 or 10,000 most common passwords which match the system's password policy) or using an external API. If using an API a zero knowledge proof or other mechanism should be used to ensure that the plain text password is not sent or used in verifying the breach status of the password. If the password is breached, the application must require the user to set a new non-breached password. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no password input | 2025-03-26 | ✅ |
| **2.1.8** | Verify that a password strength meter is provided to help users set a stronger password. | There's no password input | 2025-03-26 | ✅ |
| **2.1.9** | Verify that there are no password composition rules limiting the type of characters permitted. There should be no requirement for upper or lower case or numbers or special characters. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no password input | 2025-03-26 | ✅ |
| **2.1.10** | Verify that there are no periodic credential rotation or password history requirements. | There's no password input | 2025-03-26 | ✅ |
| **2.1.11** | Verify that "paste" functionality, browser password helpers, and external password managers are permitted. | There's no password input | 2025-03-26 | ✅ |
| **2.1.12** | Verify that the user can choose to either temporarily view the entire masked password, or temporarily view the last typed character of the password on platforms that do not have this as built-in functionality. | There's no password input | 2025-03-26 | ✅ |

## V2.2 General Authenticator Security

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.2.1** | Verify that anti-automation controls are effective at mitigating breached credential testing, brute force, and account lockout attacks. Such controls include blocking the most common breached passwords, soft lockouts, rate limiting, CAPTCHA, ever increasing delays between attempts, IP address restrictions, or risk-based restrictions such as location, first login on a device, recent attempts to unlock the account, or similar. Verify that no more than 100 failed attempts per hour is possible on a single account. | There's no password input | 2025-03-26 | ✅ |
| **2.2.2** | Verify that the use of weak authenticators (such as SMS and email) is limited to secondary verification and transaction approval and not as a replacement for more secure authentication methods. Verify that stronger methods are offered before weak methods, users are aware of the risks, or that proper measures are in place to limit the risks of account compromise. | There's no password input | 2025-03-26 | ✅ |
| **2.2.3** | Verify that secure notifications are sent to users after updates to authentication details, such as credential resets, email or address changes, logging in from unknown or risky locations. The use of push notifications - rather than SMS or email - is preferred, but in the absence of push notifications, SMS or email is acceptable as long as no sensitive information is disclosed in the notification. | There's no password input | 2025-03-26 | ✅ |
| **2.2.4** | _(not applicable to KNIME)_ | | | ✅ |
| **2.2.5** | _(not applicable to KNIME)_ | | | ✅ |
| **2.2.6** | _(not applicable to KNIME)_ | | | ✅ |
| **2.2.7** | _(not applicable to KNIME)_ | | | ✅ |

## V2.3 Authenticator Lifecycle

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.3.1** | Verify system generated initial passwords or activation codes SHOULD be securely randomly generated, SHOULD be at least 6 characters long, and MAY contain letters and numbers, and expire after a short period of time. These initial secrets must not be permitted to become the long term password. | There's no login | 2025-03-26 | ✅ |
| **2.3.2** | Verify that enrollment and use of user-provided authentication devices are supported, such as a U2F or FIDO tokens. | There's no login | 2025-03-26 | ✅ |
| **2.3.3** | Verify that renewal instructions are sent with sufficient time to renew time bound authenticators. | There's no login | 2025-03-26 | ✅ |

## V2.4 Credential Storage

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.4.1** | Verify that passwords are stored in a form that is resistant to offline attacks. Passwords SHALL be salted and hashed using an approved one-way key derivation or password hashing function. Key derivation and password hashing functions take a password, a salt, and a cost factor as inputs when generating a password hash. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No credentials are stored | 2025-03-26 | ✅ |
| **2.4.2** | Verify that the salt is at least 32 bits in length and be chosen arbitrarily to minimize salt value collisions among stored hashes. For each credential, a unique salt value and the resulting hash SHALL be stored. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No credentials are stored | 2025-03-26 | ✅ |
| **2.4.3** | Verify that if PBKDF2 is used, the iteration count SHOULD be as large as verification server performance will allow, typically at least 100,000 iterations. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No credentials are stored | 2025-03-26 | ✅ |
| **2.4.4** | Verify that if bcrypt is used, the work factor SHOULD be as large as verification server performance will allow, with a minimum of 10. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No credentials are stored | 2025-03-26 | ✅ |
| **2.4.5** | Verify that an additional iteration of a key derivation function is performed, using a salt value that is secret and known only to the verifier. Generate the salt value using an approved random bit generator [SP 800-90Ar1] and provide at least the minimum security strength specified in the latest revision of SP 800-131A. The secret salt value SHALL be stored separately from the hashed passwords (e.g., in a specialized device like a hardware security module). | No credentials are stored | 2025-03-26 | ✅ |

## V2.5 Credential Recovery

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.5.1** | Verify that a system generated initial activation or recovery secret is not sent in clear text to the user. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no login | 2025-03-26 | ✅ |
| **2.5.2** | Verify password hints or knowledge-based authentication (so-called "secret questions") are not present. | There's no login | 2025-03-26 | ✅ |
| **2.5.3** | Verify password credential recovery does not reveal the current password in any way. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no login | 2025-03-26 | ✅ |
| **2.5.4** | Verify shared or default accounts are not present (e.g. "root", "admin", or "sa"). | There's no login | 2025-03-26 | ✅ |
| **2.5.5** | Verify that if an authentication factor is changed or replaced, that the user is notified of this event. | There's no login | 2025-03-26 | ✅ |
| **2.5.6** | Verify forgotten password, and other recovery paths use a secure recovery mechanism, such as time-based OTP (TOTP) or other soft token, mobile push, or another offline recovery mechanism. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | There's no login | 2025-03-26 | ✅ |
| **2.5.7** | Verify that if OTP or multi-factor authentication factors are lost, that evidence of identity proofing is performed at the same level as during enrollment. | There's no login | 2025-03-26 | ✅ |

## V2.6 Look-up Secret Verifier

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.6.1** | Verify that lookup secrets can be used only once. | There's no login | 2025-03-26 | ✅ |
| **2.6.2** | Verify that lookup secrets have sufficient randomness (112 bits of entropy), or if less than 112 bits of entropy, salted with a unique and random 32-bit salt and hashed with an approved one-way hash. | There's no login | 2025-03-26 | ✅ |
| **2.6.3** | Verify that lookup secrets are resistant to offline attacks, such as predictable values. | There's no login | 2025-03-26 | ✅ |

## V2.7 Out of Band Verifier

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.7.1** | Verify that clear text out of band (NIST "restricted") authenticators, such as SMS or PSTN, are not offered by default, and stronger alternatives such as push notifications are offered first. | There's no login | 2025-03-26 | ✅ |
| **2.7.2** | Verify that the out of band verifier expires out of band authentication requests, codes, or tokens after 10 minutes. | There's no login | 2025-03-26 | ✅ |
| **2.7.3** | Verify that the out of band verifier authentication requests, codes, or tokens are only usable once, and only for the original authentication request. | There's no login | 2025-03-26 | ✅ |
| **2.7.4** | Verify that the out of band authenticator and verifier communicates over a secure independent channel. | There's no login | 2025-03-26 | ✅ |
| **2.7.5** | Verify that the out of band verifier retains only a hashed version of the authentication code. | There's no login | 2025-03-26 | ✅ |
| **2.7.6** | Verify that the initial authentication code is generated by a secure random number generator, containing at least 20 bits of entropy (typically a six digital random number is sufficient). | There's no login | 2025-03-26 | ✅ |

## V2.8 One Time Verifier

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.8.1** | Verify that time-based OTPs have a defined lifetime before expiring. | There's no login | 2025-03-26 | ✅ |
| **2.8.2** | Verify that symmetric keys used to verify submitted OTPs are highly protected, such as by using a hardware security module or secure operating system based key storage. | There's no login | 2025-03-26 | ✅ |
| **2.8.3** | Verify that approved cryptographic algorithms are used in the generation, seeding, and verification of OTPs. | There's no login | 2025-03-26 | ✅ |
| **2.8.4** | Verify that time-based OTP can be used only once within the validity period. | There's no login | 2025-03-26 | ✅ |
| **2.8.5** | Verify that if a time-based multi-factor OTP token is re-used during the validity period, it is logged and rejected with secure notifications being sent to the holder of the device. | There's no login | 2025-03-26 | ✅ |
| **2.8.6** | Verify physical single-factor OTP generator can be revoked in case of theft or other loss. Ensure that revocation is immediately effective across logged in sessions, regardless of location. | There's no login | 2025-03-26 | ✅ |
| **2.8.7** | Verify that biometric authenticators are limited to use only as secondary factors in conjunction with either something you have and something you know. | There's no login | 2025-03-26 | ✅ |

## V2.9 Cryptographic Verifier

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.9.1** | Verify that cryptographic keys used in verification are stored securely and protected against disclosure, such as using a Trusted Platform Module (TPM) or Hardware Security Module (HSM), or an OS service that can use this secure storage. | No verification needed | 2025-03-26 | ✅ |
| **2.9.2** | Verify that the challenge nonce is at least 64 bits in length, and statistically unique or unique over the lifetime of the cryptographic device. | No verification needed | 2025-03-26 | ✅ |
| **2.9.3** | Verify that approved cryptographic algorithms are used in the generation, seeding, and verification. | No verification needed | 2025-03-26 | ✅ |

## V2.10 Service Authentication

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **2.10.1** | Verify that intra-service secrets do not rely on unchanging credentials such as passwords, API keys or shared accounts with privileged access. | Intra-service communication is unencrypted, no secrets in use yet | 2025-03-26 | ✅ |
| **2.10.2** | Verify that if passwords are required for service authentication, the service account used is not a default credential. (e.g. root/root or admin/admin are default in some services during installation). | Intra-service communication is unencrypted, no secrets in use yet | 2025-03-26 | ✅ |
| **2.10.3** | Verify that passwords are stored with sufficient protection to prevent offline recovery attacks, including local system access. | No passwords stored | 2025-03-26 | ✅ |
| **2.10.4** | Verify passwords, integrations with databases and third-party systems, seeds and internal secrets, and API keys are managed securely and not included in the source code or stored within source code repositories. Such storage SHOULD resist offline attacks. The use of a secure software key store (L1), hardware TPM, or an HSM (L3) is recommended for password storage. | Intra-service communication between Java and Python is unencrypted, no secrets in use yet, no other integrations | 2025-03-26 | ✅ |

# V3 Session Management

## V3.1 Fundamental Session Management Security

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **3.1.1** | Verify the application never reveals session tokens in URL parameters. | No session | 2025-03-26 | ✅ |

## V3.2 Session Binding

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **3.2.1** | Verify the application generates a new session token on user authentication. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |
| **3.2.2** | Verify that session tokens possess at least 64 bits of entropy. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |
| **3.2.3** | Verify the application only stores session tokens in the browser using secure methods such as appropriately secured cookies (see section 3.4) or HTML 5 session storage. | No session | 2025-03-26 | ✅ |
| **3.2.4** | Verify that session tokens are generated using approved cryptographic algorithms. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |

## V3.3 Session Termination

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **3.3.1** | Verify that logout and expiration invalidate the session token, such that the back button or a downstream relying party does not resume an authenticated session, including across relying parties. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |
| **3.3.2** | If authenticators permit users to remain logged in, verify that re-authentication occurs periodically both when actively used or after an idle period. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |
| **3.3.3** | Verify that the application gives the option to terminate all other active sessions after a successful password change (including change via password reset/recovery), and that this is effective across the application, federated login (if present), and any relying parties. | No session | 2025-03-26 | ✅ |
| **3.3.4** | Verify that users are able to view and (having re-entered login credentials) log out of any or all currently active sessions and devices. | No session | 2025-03-26 | ✅ |

## V3.4 Cookie-based Session Management

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **3.4.1** | Verify that cookie-based session tokens have the 'Secure' attribute set. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |
| **3.4.2** | Verify that cookie-based session tokens have the 'HttpOnly' attribute set. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |
| **3.4.3** | Verify that cookie-based session tokens utilize the 'SameSite' attribute to limit exposure to cross-site request forgery attacks. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |
| **3.4.4** | _(not applicable to KNIME)_ | | | ✅ |
| **3.4.5** | Verify that if the application is published under a domain name with other applications that set or use session cookies that might disclose the session cookies, set the path attribute in cookie-based session tokens using the most precise path possible. ([C6](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No session | 2025-03-26 | ✅ |

## V3.5 Token-based Session Management

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **3.5.1** | Verify the application allows users to revoke OAuth tokens that form trust relationships with linked applications. | No session | 2025-03-26 | ✅ |
| **3.5.2** | Verify the application uses session tokens rather than static API secrets and keys, except with legacy implementations. | No session | 2025-03-26 | ✅ |
| **3.5.3** | Verify that stateless session tokens use digital signatures, encryption, and other countermeasures to protect against tampering, enveloping, replay, null cipher, and key substitution attacks. | No session | 2025-03-26 | ✅ |

## V3.6 Federated Re-authentication

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **3.6.1** | Verify that Relying Parties (RPs) specify the maximum authentication time to Credential Service Providers (CSPs) and that CSPs re-authenticate the user if they haven't used a session within that period. | No session | 2025-03-26 | ✅ |
| **3.6.2** | _(not applicable to KNIME)_ | | | ✅ |

## V3.7 Defenses Against Session Management Exploits

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **3.7.1** | Verify the application ensures a full, valid login session or requires re-authentication or secondary verification before allowing any sensitive transactions or account modifications. | No session | 2025-03-26 | ✅ |

# V4 Access Control

## V4.1 General Access Control Design

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **4.1.1** | Verify that the application enforces access control rules on a trusted service layer, especially if client-side access control is present and could be bypassed. | | 2025-03-26 | ✅ |
| **4.1.2** | Verify that all user and data attributes and policy information used by access controls cannot be manipulated by end users unless specifically authorized. | | 2025-03-26 | ✅ |
| **4.1.3** | Verify that the principle of least privilege exists - users should only be able to access functions, data files, URLs, controllers, services, and other resources, for which they possess specific authorization. This implies protection against spoofing and elevation of privilege. ([C7](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Script node deliberately allows users full access | 2025-03-26 | ✅ |
| **4.1.4** | [DELETED, DUPLICATE OF 4.1.3] | | 2025-03-26 | ✅ |
| **4.1.5** | Verify that access controls fail securely including when an exception occurs. ([C10](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2025-03-26 | ✅ |

## V4.2 Operation Level Access Control

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **4.2.1** | Verify that sensitive data and APIs are protected against Insecure Direct Object Reference (IDOR) attacks targeting creation, reading, updating and deletion of records, such as creating or updating someone else's record, viewing everyone's records, or deleting all records. | Script node deliberately allows users full access | 2025-03-26 | ✅ |
| **4.2.2** | Verify that the application or framework enforces a strong anti-CSRF mechanism to protect authenticated functionality, and effective anti-automation or anti-CSRF protects unauthenticated functionality.<br>_This only applies to web pages that our application control. For web pages created by data apps we cannot ensure this._ | | 2025-03-26 | ✅ |

## V4.3 Other Access Control Considerations

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **4.3.1** | Verify administrative interfaces use appropriate multi-factor authentication to prevent unauthorized use. | | 2025-03-26 | ✅ |
| **4.3.2** | Verify that directory browsing is disabled unless deliberately desired. Additionally, applications should not allow discovery or disclosure of file or directory metadata, such as Thumbs.db, .DS_Store, .git or .svn folders. | | 2025-03-26 | ✅ |
| **4.3.3** | _(not applicable to KNIME)_ | | | ✅ |

# V5 Validation, Sanitization and Encoding

## V5.1 Input Validation

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **5.1.1** | Verify that the application has defenses against HTTP parameter pollution attacks, particularly if the application framework makes no distinction about the source of request parameters (GET, POST, cookies, headers, or environment variables). |  | 2024-12-18 | ✅ |
| **5.1.2** | Verify that frameworks protect against mass parameter assignment attacks, or that the application has countermeasures to protect against unsafe parameter assignment, such as marking fields private or similar. ([C5](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Checked by Sonar rule [S4684](https://rules.sonarsource.com/java/RSPEC-4684/) for Java programs. But not applicable to Python script nodes anyways.| 2024-12-18 | ✅ |
| **5.1.3** | Verify that all input (HTML form fields, REST requests, URL parameters, HTTP headers, cookies, batch files, RSS feeds, etc) is validated using positive validation (allow lists). ([C5](https://owasp.org/www-project-proactive-controls/#div-numbering)) | The script (which can be input via flow variable) is not restricted via an allow-list because it is supposed to be flexible. Table inputs are safe, but (pickled) port objects could be read from unsafe sources if the workflow builder does so explicitly. Improved node column parameter input validation. | 2024-12-18 | |
| **5.1.4** | Verify that structured data is strongly typed and validated against a defined schema including allowed characters, length and pattern (e.g. credit card numbers, e-mail addresses, telephone numbers, or validating that two related fields are reasonable, such as checking that suburb and zip/postcode match). ([C5](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Structured data types known to KNIME are typed and validated by KNIME, structured types not known to KNIME need to be represented as unstructured data anyways. | 2024-12-18 | ✅ |
| **5.1.5** | Verify that URL redirects and forwards only allow destinations which appear on an allow list, or show a warning when redirecting to potentially untrusted content. | Checked by Sonar rule [S5146](https://rules.sonarsource.com/java/RSPEC-5146/) for Java. Download redirects are all generated by S3 SDK. Other redirects use list of allowed hosts. Our Python framework doesn't access URLs, if the user does so in scripts, they need to verify their redirects themselves. | 2024-12-18 | ✅ |


## V5.2 Sanitization and Sandboxing

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **5.2.1** | Verify that all untrusted HTML input from WYSIWYG editors or similar is properly sanitized with an HTML sanitizer library or framework feature. ([C5](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2024-12-18 | ✅ |
| **5.2.2** | Verify that unstructured data is sanitized to enforce safety measures such as allowed characters and length. | | 2024-12-18 | ✅ |
| **5.2.3** | Verify that the application sanitizes user input before passing to mail systems to protect against SMTP or IMAP injection. | | 2024-12-18 | ✅ |
| **5.2.4** | Verify that the application avoids the use of eval() or other dynamic code execution features. Where there is no alternative, any user input being included must be sanitized or sandboxed before being executed.<br>_Scripting nodes in workflows are exempted because we don't have control over the output._ | Checked by Sonar rule [S5334](https://rules.sonarsource.com/java/RSPEC-5334/) for Java programs. In the long run, Python script execution should be sandboxed on executors. | 2024-12-18 | |
| **5.2.5** | Verify that the application protects against template injection attacks by ensuring that any user input being included is sanitized or sandboxed. | Currently no templating in place. | 2024-12-18 | |
| **5.2.6** | Verify that the application protects against SSRF attacks, by validating or sanitizing untrusted data or HTTP file metadata, such as filenames and URL input fields, and uses allow lists of protocols, domains, paths and ports. | | 2024-12-18 | ✅ |
| **5.2.7** | Verify that the application sanitizes, disables, or sandboxes user-supplied Scalable Vector Graphics (SVG) scriptable content, especially as they relate to XSS resulting from inline scripts, and foreignObject. | Improved Python View | 2024-12-18 | |
| **5.2.8** | Verify that the application sanitizes, disables, or sandboxes user-supplied scriptable or expression template language content, such as Markdown, CSS or XSL stylesheets, BBCode, or similar. | We do sanitize user-supplied Markdown for node and parameter descriptions and disallow HTML. | 2024-12-18 | |

## V5.3 Output Encoding and Injection Prevention

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **5.3.1** | Verify that output encoding is relevant for the interpreter and context required. For example, use encoders specifically for HTML values, HTML attributes, JavaScript, URL parameters, HTTP headers, SMTP, and others as the context requires, especially from untrusted inputs (e.g. names with Unicode or apostrophes, such as Ã£ÂÂ­Ã£Ââ€œ or O'Hara). ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2024-12-18 | |
| **5.3.2** | Verify that output encoding preserves the user's chosen character set and locale, such that any Unicode character point is valid and safely handled. ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | The user can write files from scripts, but it's their responsibility to set the encoding. Otherwise not applicable as we do not provide output files. | 2024-12-18 | |
| **5.3.3** | Verify that context-aware, preferably automated - or at worst, manual - output escaping protects against reflected, stored, and DOM based XSS. ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | This was an issue of Python Views at some point, but was fixed by sandboxing. | 2024-12-18 | |
| **5.3.4** | Verify that data selection or database queries (e.g. SQL, HQL, ORM, NoSQL) use parameterized queries, ORMs, entity frameworks, or are otherwise protected from database injection attacks. ([C3](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Checked by Sonar rules [S3649](https://rules.sonarsource.com/java/RSPEC-3649/) and [S2077](https://rules.sonarsource.com/java/RSPEC-2077/) for Java programs. | 2024-12-18 | ✅ |
| **5.3.5** | Verify that where parameterized or safer mechanisms are not present, context-specific output encoding is used to protect against injection attacks, such as the use of SQL escaping to protect against SQL injection. ([C3, C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Users have access to raw unsanitized input strings in columns and flow variables. If they use these when constructing an HTML view, we document that they need to be aware that e.g. strings can contain HTML code. | 2024-12-18 | |
| **5.3.6** | Verify that the application protects against JSON injection attacks, JSON eval attacks, and JavaScript expression evaluation. ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Checked by Sonar rules [S6398](https://rules.sonarsource.com/java/RSPEC-6398/) for Java programs. JSON handling in Python is mostly performed via dictionaries and not prone to injection attacks. | 2024-12-18 | |
| **5.3.7** | Verify that the application protects against LDAP injection vulnerabilities, or that specific security controls to prevent LDAP injection have been implemented. ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Checked by Sonar rules [2078](https://rules.sonarsource.com/java/RSPEC-2078/) for Java programs. | 2024-12-18 | ✅ |
| **5.3.8** | Verify that the application protects against OS command injection and that operating system calls use parameterized OS queries or use contextual command line output encoding. ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Checked by Sonar rules [S2076](https://rules.sonarsource.com/java/RSPEC-2076/), [S5883](https://rules.sonarsource.com/java/RSPEC-5883/), and [S6350](https://rules.sonarsource.com/java/RSPEC-6350/) for Java programs. No subprocess or system calls in Python. | 2024-12-18 | |
| **5.3.9** | Verify that the application protects against Local File Inclusion (LFI) or Remote File Inclusion (RFI) attacks. | Checked by Sonar rules [2083](https://rules.sonarsource.com/java/RSPEC-2083/) for Java programs. | 2024-12-18 | |
| **5.3.10** | Verify that the application protects against XPath injection or XML injection attacks. ([C4](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Checked by Sonar rules [S6399](https://rules.sonarsource.com/java/RSPEC-6399/), [S2091](https://rules.sonarsource.com/java/RSPEC-2091/),  [S2755](https://rules.sonarsource.com/java/RSPEC-2755/), and [S6374](https://rules.sonarsource.com/java/RSPEC-6374/) for Java programs. | 2024-12-18 | |

## V5.4 Memory, String, and Unmanaged Code
_(not applicable to KNIME)_

## V5.5 Deserialization Prevention

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **5.5.1** | Verify that serialized objects use integrity checks or are encrypted to prevent hostile object creation or data tampering. ([C5](https://owasp.org/www-project-proactive-controls/#div-numbering)) | As long as the user doesn't read data from untrusted sources, all serialization and deserialization happens in the KNIMEverse only. | 2024-12-18 | |
| **5.5.2** | Verify that the application correctly restricts XML parsers to only use the most restrictive configuration possible and to ensure that unsafe features such as resolving external entities are disabled to prevent XML eXternal Entity (XXE) attacks. | Checked by Sonar rules [S2755](https://rules.sonarsource.com/java/RSPEC-2755/) and [S6374](https://rules.sonarsource.com/java/RSPEC-6374/) for Java programs. Cannot use too restrictive configurations for working with XML cells in the table. | 2024-12-18 | |
| **5.5.3** | Verify that deserialization of untrusted data is avoided or is protected in both custom code and third-party libraries (such as JSON, XML and YAML parsers). | Checked by Sonar rule [S5135](https://rules.sonarsource.com/java/RSPEC-5135/) for Java programs. | 2024-12-18 | |
| **5.5.4** | Verify that when parsing JSON in browsers or JavaScript-based backends, JSON.parse is used to parse the JSON document. Do not use eval() to parse JSON. | | 2024-12-18 | |

# V6 Stored Cryptography

## V6.1 Data Classification

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **6.1.1** | Verify that regulated private data is stored encrypted while at rest, such as Personally Identifiable Information (PII), sensitive personal information, or data assessed likely to be subject to EU's GDPR. | | | |
| **6.1.2** | _(not applicable to KNIME)_ | | | ✅ |
| **6.1.3** | _(not applicable to KNIME)_ | | | ✅ |

## V6.2 Algorithms

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **6.2.1** | Verify that all cryptographic modules fail securely, and errors are handled in a way that does not enable Padding Oracle attacks. | | | |
| **6.2.2** | Verify that industry proven or government approved cryptographic algorithms, modes, and libraries are used, instead of custom coded cryptography. ([C8](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **6.2.3** | Verify that encryption initialization vector, cipher configuration, and block modes are configured securely using the latest advice. | | | |
| **6.2.4** | Verify that random number, encryption or hashing algorithms, key lengths, rounds, ciphers or modes, can be reconfigured, upgraded, or swapped at any time, to protect against cryptographic breaks. ([C8](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **6.2.5** | Verify that known insecure block modes (i.e. ECB, etc.), padding modes (i.e. PKCS#1 v1.5, etc.), ciphers with small block sizes (i.e. Triple-DES, Blowfish, etc.), and weak hashing algorithms (i.e. MD5, SHA1, etc.) are not used unless required for backwards compatibility. | | | |
| **6.2.6** | Verify that nonces, initialization vectors, and other single use numbers must not be used more than once with a given encryption key. The method of generation must be appropriate for the algorithm being used. | | | |
| **6.2.7** | _(not applicable to KNIME)_ | | | ✅ |
| **6.2.8** | _(not applicable to KNIME)_ | | | ✅ |

## V6.3 Random Values

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **6.3.1** | Verify that all random numbers, random file names, random GUIDs, and random strings are generated using the cryptographic module's approved cryptographically secure random number generator when these random values are intended to be not guessable by an attacker. | | | |
| **6.3.2** | Verify that random GUIDs are created using the GUID v4 algorithm, and a Cryptographically-secure Pseudo-random Number Generator (CSPRNG). GUIDs created using other pseudo-random number generators may be predictable. | | | |
| **6.3.3** | _(not applicable to KNIME)_ | | | ✅ |

## V6.4 Secret Management

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **6.4.1** | Verify that a secrets management solution such as a key vault is used to securely create, store, control access to and destroy secrets. ([C8](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **6.4.2** | Verify that key material is not exposed to the application but instead uses an isolated security module like a vault for cryptographic operations. ([C8](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |

# V7 Error Handling and Logging

## V7.1 Log Content

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **7.1.1** | Verify that the application does not log credentials or payment details. Session tokens should only be stored in logs in an irreversible, hashed form. ([C9, C10](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2025-03-26 | |
| **7.1.2** | Verify that the application does not log other sensitive data as defined under local privacy laws or relevant security policy. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2025-03-26 | |
| **7.1.3** | Verify that the application logs security relevant events including successful and failed authentication events, access control failures, deserialization failures and input validation failures. ([C5, C7](https://owasp.org/www-project-proactive-controls/#div-numbering)) | No user login. If script tries to connect to an external service, that needs to log security relevant events. | 2025-03-26 | ✅ |
| **7.1.4** | Verify that each log event includes necessary information that would allow for a detailed investigation of the timeline when an event happens. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | See above. | 2025-03-26 | ✅ |

## V7.2 Log Processing

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **7.2.1** | Verify that all authentication decisions are logged, without storing sensitive session tokens or passwords. This should include requests with relevant metadata needed for security investigations. | | 2025-03-26 | ✅ |
| **7.2.2** | Verify that all access control decisions can be logged and all failed decisions are logged. This should include requests with relevant metadata needed for security investigations. | | 2025-03-26 | ✅ |

## V7.3 Log Protection

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **7.3.1** | Verify that all logging components appropriately encode data to prevent log injection. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2025-03-26  | |
| **7.3.2** | [DELETED, DUPLICATE OF 7.3.1] | | | |
| **7.3.3** | Verify that security logs are protected from unauthorized access and modification. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2025-03-26 | ✅ |
| **7.3.4** | Verify that time sources are synchronized to the correct time and time zone. Strongly consider logging only in UTC if systems are global to assist with post-incident forensic analysis. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Python script logs have a `System.currentTimeMillis()` timestamp ✅, other logs use the KNIME log | 2025-03-26 | |

## V7.4 Error Handling

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **7.4.1** | Verify that a generic message is shown when an unexpected or security sensitive error occurs, potentially with a unique ID which support personnel can use to investigate. ([C10](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2025-03-26  | ✅ |
| **7.4.2** | Verify that exception handling (or a functional equivalent) is used across the codebase to account for expected and unexpected error conditions. ([C10](https://owasp.org/www-project-proactive-controls/#div-numbering)) | User-friendliness of messages could be improved. | 2025-03-26 | |
| **7.4.3** | Verify that a "last resort" error handler is defined which will catch all unhandled exceptions. ([C10](https://owasp.org/www-project-proactive-controls/#div-numbering)) | KNIME makes sure the application doesn't crash. | 2025-03-26 | |

# V8 Data Protection

## V8.1 General Data Protection

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **8.1.1** | Verify the application protects sensitive data from being cached in server components such as load balancers and application caches. | | | |
| **8.1.2** | Verify that all cached or temporary copies of sensitive data stored on the server are protected from unauthorized access or purged/invalidated after the authorized user accesses the sensitive data. | | | |
| **8.1.3** | Verify the application minimizes the number of parameters in a request, such as hidden fields, Ajax variables, cookies and header values. | | | |
| **8.1.4** | Verify the application can detect and alert on abnormal numbers of requests, such as by IP, user, total per hour or day, or whatever makes sense for the application. | | | |
| **8.1.5** | Verify that regular backups of important data are performed and that test restoration of data is performed. | | | |
| **8.1.6** | Verify that backups are stored securely to prevent data from being stolen or corrupted. | | | |

## V8.2 Client-side Data Protection

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **8.2.1** | Verify the application sets sufficient anti-caching headers so that sensitive data is not cached in modern browsers. | | | |
| **8.2.2** | Verify that data stored in browser storage (such as localStorage, sessionStorage, IndexedDB, or cookies) does not contain sensitive data. | | | |
| **8.2.3** | Verify that authenticated data is cleared from client storage, such as the browser DOM, after the client or session is terminated. | | | |

## V8.3 Sensitive Private Data

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **8.3.1** | Verify that sensitive data is sent to the server in the HTTP message body or headers, and that query string parameters from any HTTP verb do not contain sensitive data. | | | |
| **8.3.2** | Verify that users have a method to remove or export their data on demand. | | | |
| **8.3.3** | Verify that users are provided clear language regarding collection and use of supplied personal information and that users have provided opt-in consent for the use of that data before it is used in any way. | | | |
| **8.3.4** | Verify that all sensitive data created and processed by the application has been identified, and ensure that a policy is in place on how to deal with sensitive data. ([C8](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **8.3.5** | Verify accessing sensitive data is audited (without logging the sensitive data itself), if the data is collected under relevant data protection directives or where logging of access is required. | | | |
| **8.3.6** | _(not applicable to KNIME)_ | | | ✅ |
| **8.3.7** | Verify that sensitive or private information that is required to be encrypted, is encrypted using approved algorithms that provide both confidentiality and integrity. ([C8](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **8.3.8** | Verify that sensitive personal information is subject to data retention classification, such that old or out of date data is deleted automatically, on a schedule, or as the situation requires. | | | |

# V9 Communication

## V9.1 Client Communication Security

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **9.1.1** | Verify that TLS is used for all client connectivity, and does not fall back to insecure or unencrypted communications. ([C8](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | | |
| **9.1.2** | Verify using up to date TLS testing tools that only strong cipher suites are enabled, with the strongest cipher suites set as preferred. | | | |
| **9.1.3** | Verify that only the latest recommended versions of the TLS protocol are enabled, such as TLS 1.2 and TLS 1.3. The latest version of the TLS protocol should be the preferred option. | | | |

## V9.2 Server Communication Security

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **9.2.1** | Verify that connections to and from the server use trusted TLS certificates. Where internally generated or self-signed certificates are used, the server must be configured to only trust specific internal CAs and specific self-signed certificates. All others should be rejected. | | | |
| **9.2.2** | Verify that encrypted communications such as TLS is used for all inbound and outbound connections, including for management ports, monitoring, authentication, API, or web service calls, database, cloud, serverless, mainframe, external, and partner connections. The server must not fall back to insecure or unencrypted protocols. | | | |
| **9.2.3** | Verify that all encrypted connections to external systems that involve sensitive information or functions are authenticated.<br>_Connections originating from nodes in workflows are out of our control._ | | | |
| **9.2.4** | _(not applicable to KNIME)_ | | | ✅ |
| **9.2.5** | _(not applicable to KNIME)_ | | | ✅ |

# V10 Malicious Code

## V10.1 Code Integrity

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **10.1.1** | _(not applicable to KNIME)_ | | | ✅ |

## V10.2 Malicious Code Search

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **10.2.1** | Verify that the application source code and third party libraries do not contain unauthorized phone home or data collection capabilities. Where such functionality exists, obtain the user's permission for it to operate before collecting any data. | | | |
| **10.2.2** | _(not applicable to KNIME)_ | | | ✅ |
| **10.2.3** | Verify that the application source code and third party libraries do not contain back doors, such as hard-coded or additional undocumented accounts or keys, code obfuscation, undocumented binary blobs, rootkits, or anti-debugging, insecure debugging features, or otherwise out of date, insecure, or hidden functionality that could be used maliciously if discovered. | | | |
| **10.2.4** | Verify that the application source code and third party libraries do not contain time bombs by searching for date and time related functions. | | | |
| **10.2.5** | Verify that the application source code and third party libraries do not contain malicious code, such as salami attacks, logic bypasses, or logic bombs. | | | |
| **10.2.6** | Verify that the application source code and third party libraries do not contain Easter eggs or any other potentially unwanted functionality. | | | |

## V10.3 Application Integrity

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **10.3.1** | Verify that if the application has a client or server auto-update feature, updates should be obtained over secure channels and digitally signed. The update code must validate the digital signature of the update before installing or executing the update. | | | |
| **10.3.2** | Verify that the application employs integrity protections, such as code signing or subresource integrity. The application must not load or execute code from untrusted sources, such as loading includes, modules, plugins, code, or libraries from untrusted sources or the Internet.<br>_KNIME Analytics Platform allows installing unsigned extensions but requires explicit confirmation._ | | | |
| **10.3.3** | Verify that the application has protection from subdomain takeovers if the application relies upon DNS entries or DNS subdomains, such as expired domain names, out of date DNS pointers or CNAMEs, expired projects at public source code repos, or transient cloud APIs, serverless functions, or storage buckets (*autogen-bucket-id*.cloud.example.com) or similar. Protections can include ensuring that DNS names used by applications are regularly checked for expiry or change. | | | |

# V11 Business Logic

## V11.1 Business Logic Security

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **11.1.1** | Verify that the application will only process business logic flows for the same user in sequential step order and without skipping steps.| | 2025-03-26 | |
| **11.1.2** | Verify that the application will only process business logic flows with all steps being processed in realistic human time, i.e. transactions are not submitted too quickly.| | 2025-03-26 | |
| **11.1.3** | Verify the application has appropriate limits for specific business actions or transactions which are correctly enforced on a per user basis. | | 2025-03-26 | |
| **11.1.4** | Verify that the application has anti-automation controls to protect against excessive calls such as mass data exfiltration, business logic requests, file uploads or denial of service attacks. | | 2025-03-26 | |
| **11.1.5** | Verify the application has business logic limits or validation to protect against likely business risks or threats, identified using threat modeling or similar methodologies. | | 2025-03-26 | |
| **11.1.6** | Verify that the application does not suffer from "Time Of Check to Time Of Use" (TOCTOU) issues or other race conditions for sensitive operations. | | 2025-03-26 | |
| **11.1.7** | Verify that the application monitors for unusual events or activity from a business logic perspective. For example, attempts to perform actions out of order or actions which a normal user would never attempt. ([C9](https://owasp.org/www-project-proactive-controls/#div-numbering)) | | 2025-03-26 | |
| **11.1.8** | Verify that the application has configurable alerting when automated attacks or unusual activity is detected. | | 2025-03-26 | |

# V12 Files and Resources

## V12.1 File Upload

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **12.1.1** | Verify that the application will not accept large files that could fill up storage or cause a denial of service. | No file upload. User is responsible for script | 2025-03-26 | ✅ |
| **12.1.2** | Verify that the application checks compressed files (e.g. zip, gz, docx, odt) against maximum allowed uncompressed size and against maximum number of files before uncompressing the file. | No file upload. User is responsible for script | 2025-03-26 | ✅ |
| **12.1.3** | Verify that a file size quota and maximum number of files per user is enforced to ensure that a single user cannot fill up the storage with too many files, or excessively large files. | No file upload. User is responsible for script | 2025-03-26 | ✅ |

## V12.2 File Integrity

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **12.2.1** | Verify that files obtained from untrusted sources are validated to be of expected type based on the file's content. | No file upload. User is responsible for script | 2025-03-26 | ✅ |

## V12.3 File Execution

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **12.3.1** | Verify that user-submitted filename metadata is not used directly by system or framework filesystems and that a URL API is used to protect against path traversal. | | 2025-03-26 | ✅ |
| **12.3.2** | Verify that user-submitted filename metadata is validated or ignored to prevent the disclosure, creation, updating or removal of local files (LFI). | | 2025-03-26 | ✅ |
| **12.3.3** | Verify that user-submitted filename metadata is validated or ignored to prevent the disclosure or execution of remote files via Remote File Inclusion (RFI) or Server-side Request Forgery (SSRF) attacks. | | 2025-03-26 | ✅ |
| **12.3.4** | Verify that the application protects against Reflective File Download (RFD) by validating or ignoring user-submitted filenames in a JSON, JSONP, or URL parameter, the response Content-Type header should be set to text/plain, and the Content-Disposition header should have a fixed filename. | | 2025-03-26 | ✅ |
| **12.3.5** | Verify that untrusted file metadata is not used directly with system API or libraries, to protect against OS command injection. | | 2025-03-26 | ✅ |
| **12.3.6** | Verify that the application does not include and execute functionality from untrusted sources, such as unverified content distribution networks, JavaScript libraries, node npm libraries, or server-side DLLs. | User is responsible for script | 2025-03-26 | ✅ |

## V12.4 File Storage

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **12.4.1** | Verify that files obtained from untrusted sources are stored outside the web root, with limited permissions. | No web root | 2025-03-26 | ✅ |
| **12.4.2** | Verify that files obtained from untrusted sources are scanned by antivirus scanners to prevent upload and serving of known malicious content. | Antivirus configuration is up to the user | 2025-03-26 | ✅ |

## V12.5 File Download

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **12.5.1** | Verify that the web tier is configured to serve only files with specific file extensions to prevent unintentional information and source code leakage. For example, backup files (e.g. .bak), temporary working files (e.g. .swp), compressed files (.zip, .tar.gz, etc) and other extensions commonly used by editors should be blocked unless required. | Only whitelisted files are served | 2025-03-26 | |
| **12.5.2** | Verify that direct requests to uploaded files will never be executed as HTML/JavaScript content. | No upload possible | 2025-03-26 | ✅ |

## V12.6 SSRF Protection

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **12.6.1** | Verify that the web or application server is configured with an allow list of resources or systems to which the server can send requests or load data/files from. | | 2025-03-26 | ✅ |

# V13 API and Web Service

## V13.1 Generic Web Service Security

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **13.1.1** | Verify that all application components use the same encodings and parsers to avoid parsing attacks that exploit different URI or file parsing behavior that could be used in SSRF and RFI attacks. | No Web API | 2025-03-26 | ✅ |
| **13.1.2** | [DELETED, DUPLICATE OF 4.3.1] | No Web API | 2025-03-26 | ✅ |
| **13.1.3** | Verify API URLs do not expose sensitive information, such as the API key, session tokens etc. | No Web API | 2025-03-26 | ✅ |
| **13.1.4** | Verify that authorization decisions are made at both the URI, enforced by programmatic or declarative security at the controller or router, and at the resource level, enforced by model-based permissions. | No Web API | 2025-03-26 | ✅ |
| **13.1.5** | Verify that requests containing unexpected or missing content types are rejected with appropriate headers (HTTP response status 406 Unacceptable or 415 Unsupported Media Type). | No Web API | 2025-03-26 | ✅ |

## V13.2 RESTful Web Service

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **13.2.1** | Verify that enabled RESTful HTTP methods are a valid choice for the user or action, such as preventing normal users using DELETE or PUT on protected API or resources. | No Web API | 2025-03-26 | ✅ |
| **13.2.2** | Verify that JSON schema validation is in place and verified before accepting input. | No Web API | 2025-03-26 | ✅ |
| **13.2.3** | Verify that RESTful web services that utilize cookies are protected from Cross-Site Request Forgery via the use of at least one or more of the following: double submit cookie pattern, CSRF nonces, or Origin request header checks. | No Web API | 2025-03-26 | ✅ |
| **13.2.4** | [DELETED, DUPLICATE OF 11.1.4] | No Web API | 2025-03-26 | ✅ |
| **13.2.5** | Verify that REST services explicitly check the incoming Content-Type to be the expected one, such as application/xml or application/json. | No Web API | 2025-03-26 | ✅ |
| **13.2.6** | Verify that the message headers and payload are trustworthy and not modified in transit. Requiring strong encryption for transport (TLS only) may be sufficient in many cases as it provides both confidentiality and integrity protection. Per-message digital signatures can provide additional assurance on top of the transport protections for high-security applications but bring with them additional complexity and risks to weigh against the benefits. | No Web API | 2025-03-26 | ✅ |

## V13.3 SOAP Web Service
_(not applicable to KNIME)_

## V13.4 GraphQL
_(not applicable to KNIME)_

# V14 Configuration

## V14.1 Build and Deploy

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **14.1.1** | Verify that the application build and deployment processes are performed in a secure and repeatable way, such as CI / CD automation, automated configuration management, and automated deployment scripts. | | 2025-03-26 | |
| **14.1.2** | _(not applicable to KNIME)_ | | | ✅ |
| **14.1.3** | Verify that server configuration is hardened as per the recommendations of the application server and frameworks in use. | DevOps responsibility | 2025-03-26 | ✅ |
| **14.1.4** | Verify that the application, configuration, and all dependencies can be re-deployed using automated deployment scripts, built from a documented and tested runbook in a reasonable time, or restored from backups in a timely fashion. | | 2025-03-26 | |
| **14.1.5** | _(not applicable to KNIME)_ | | | ✅ |

## V14.2 Dependency

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **14.2.1** | Verify that all components are up to date, preferably using a dependency checker during build or compile time. ([C2](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Frontend code is checked with `npm audit` for vulnerabilities, the Python environment is checked by DependencyTrack. | 2025-03-26 | |
| **14.2.2** | Verify that all unneeded features, documentation, sample applications and configurations are removed. | | 2025-03-26 | |
| **14.2.3** | Verify that if application assets, such as JavaScript libraries, CSS or web fonts, are hosted externally on a Content Delivery Network (CDN) or external provider, Subresource Integrity (SRI) is used to validate the integrity of the asset. | | 2025-03-26 | ✅ |
| **14.2.4** | Verify that third party components come from pre-defined, trusted and continually maintained repositories. ([C2](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Java dependencies come from the target platform or we have ship a fixed and trusted version. Frontend code uses packages from the trusted source https://npmjs.com, and Python packages (via `knime-conda-channels`) come from `conda-forge` with our self-maintained metapackages `knime-python-scripting`. | 2025-03-26 | |
| **14.2.5** | Verify that a Software Bill of Materials (SBOM) is maintained of all third party libraries in use. ([C2](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Frontend SBOM is exported during the build, Bundled Python environment SBOM is created in `knime-conda-channels` and Java code is checked via the `knime-full` docker image in DependencyTrack | 2025-03-26 | |
| **14.2.6** | Verify that the attack surface is reduced by sandboxing or encapsulating third party libraries to expose only the required behaviour into the application. ([C2](https://owasp.org/www-project-proactive-controls/#div-numbering)) | Python scripts are sandboxed in the sense that they run in a dedicated process, but still on the same machine. Other third party libs like Py4J cannot be sandboxed as they are integral to the communication between Python and the JVM. | 2025-03-26 | |

## V14.3 Unintended Security Disclosure

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **14.3.1** | [DELETED, DUPLICATE OF 7.4.1] | | | |
| **14.3.2** | Verify that web or application server and application framework debug modes are disabled in production to eliminate debug features, developer consoles, and unintended security disclosures. | | 2025-03-26 | |
| **14.3.3** | Verify that the HTTP headers or any part of the HTTP response do not expose detailed version information of system components. | | 2025-03-26 | ✅ |

## V14.4 HTTP Security Headers

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **14.4.1** | Verify that every HTTP response contains a Content-Type header. Also specify a safe character set (e.g., UTF-8, ISO-8859-1) if the content types are text/*, /+xml and application/xml. Content must match with the provided Content-Type header. | | 2025-03-26 | |
| **14.4.2** | Verify that all API responses contain a Content-Disposition: attachment; filename="api.json" header (or other appropriate filename for the content type). | Handled by UIExtension framework | 2025-03-26 | |
| **14.4.3** | Verify that a Content Security Policy (CSP) response header is in place that helps mitigate impact for XSS attacks like HTML, DOM, JSON, and JavaScript injection vulnerabilities. | Handled by UIExtension framework | 2025-03-26 | |
| **14.4.4** | Verify that all responses contain a X-Content-Type-Options: nosniff header. | Handled by UIExtension framework | 2025-03-26 | |
| **14.4.5** | Verify that a Strict-Transport-Security header is included on all responses and for all subdomains, such as Strict-Transport-Security: max-age=15724800; includeSubdomains. | Handled by UIExtension framework | 2025-03-26 | |
| **14.4.6** | Verify that a suitable Referrer-Policy header is included to avoid exposing sensitive information in the URL through the Referer header to untrusted parties. | Handled by UIExtension framework | 2025-03-26 | |
| **14.4.7** | Verify that the content of a web application cannot be embedded in a third-party site by default and that embedding of the exact resources is only allowed where necessary by using suitable Content-Security-Policy: frame-ancestors and X-Frame-Options response headers. | Handled by UIExtension framework | 2025-03-26 | |

## V14.5 HTTP Request Header Validation

| # | Description | Notes | Last Checked | N/A |
| :---: | :--- | :---- | :---: | :---: |
| **14.5.1** | Verify that the application server only accepts the HTTP methods in use by the application/API, including pre-flight OPTIONS, and logs/alerts on any requests that are not valid for the application context. | Handled by UIExtension framework | 2025-03-26 | |
| **14.5.2** | Verify that the supplied Origin header is not used for authentication or access control decisions, as the Origin header can easily be changed by an attacker. | | 2025-03-26 | ✅ |
| **14.5.3** | Verify that the Cross-Origin Resource Sharing (CORS) Access-Control-Allow-Origin header uses a strict allow list of trusted domains and subdomains to match against and does not support the "null" origin. | | 2025-03-26 | ✅ |
| **14.5.4** | Verify that HTTP headers added by a trusted proxy or SSO devices, such as a bearer token, are authenticated by the application. | | 2025-03-26 | ✅ |
