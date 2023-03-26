import os

from azure.identity import (
    AuthenticationRecord,
    DeviceCodeCredential,
    TokenCachePersistenceOptions,
)
from kiota_abstractions.serialization import Parsable
from kiota_authentication_azure.azure_identity_authentication_provider import (
    AzureIdentityAuthenticationProvider,
)
from kiota_serialization_json.json_serialization_writer import JsonSerializationWriter
from msgraph import GraphRequestAdapter, GraphServiceClient


class GraphCredential(DeviceCodeCredential):
    def __init__(self, api: 'GraphAPI', *args, **kwargs) -> None:
        self._api = api
        super().__init__(*args, **kwargs)

    def authenticate(self, **kwargs) -> AuthenticationRecord:
        auth = super().authenticate(**kwargs)
        self._api._store_auth(auth)
        return auth


class GraphAPI:
    def __init__(
        self,
        client_id,
        scopes,
        authentication_record_file: str | None = None,
        allow_unencrypted_storage: bool = False,
    ):
        persist = TokenCachePersistenceOptions(
            allow_unencrypted_storage=allow_unencrypted_storage
        )
        self.authentication_record_file = authentication_record_file
        auth = None
        if authentication_record_file and os.path.exists(authentication_record_file):
            with open(authentication_record_file) as f:
                auth = AuthenticationRecord.deserialize(f.read())
        self.credential = GraphCredential(
            self,
            client_id=client_id,
            cache_persistence_options=persist,
            authentication_record=auth,
        )
        auth_provider = AzureIdentityAuthenticationProvider(
            self.credential, scopes=scopes
        )
        adapter = GraphRequestAdapter(auth_provider)
        self.client = GraphServiceClient(adapter)
        # msgraph.generated.sites.sites_request_builder.SitesRequestBuilder = self.client.sites

    def _store_auth(self, auth: AuthenticationRecord):
        if self.authentication_record_file:
            with open('auth.json', 'w') as f:
                f.write(auth.serialize())

    def __getattribute__(self, name: str):
        try:
            return super().__getattribute__(name)
        except:
            return getattr(self.client, name)

    @staticmethod
    def to_json(item: Parsable):
        writer = JsonSerializationWriter()
        item.serialize(writer)
        return writer.writer


CLIENT_ID = 'bcc98b3d-df9e-43ac-929c-08b5b7b07648'
SCOPES = ['User.Read', 'Sites.ReadWrite.All']

gr = GraphAPI(CLIENT_ID, SCOPES)
gr.client.sites_by_id('bssgj.sharepoint.com').get_by_path_with_path('/sites/SpringHouse')
