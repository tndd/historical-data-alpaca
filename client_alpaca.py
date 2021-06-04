from dataclasses import dataclass


@dataclass
class ClientAlpaca:
    _api_key: str
    _secret_key: str
    _base_url: str
    _dl_destination_path = './data'

    def get_auth_headers(self) -> dict:
        return {
            "APCA-API-KEY-ID": self._api_key,
            "APCA-API-SECRET-KEY": self._secret_key
        }
