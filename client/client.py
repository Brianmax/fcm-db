import requests


class EncompassClient:
    def __init__(self, api_server, client_id, client_secret, username, password, instance):
        self.api_server = api_server
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = f"{username} @encompass: {instance}"
        self.password = password
        self.access_token = None

    def get_access_token(self):
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }

        response = requests.post(
            f"{self.api_server}/oauth2/v1/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        if response.status_code == 200:
            res = response.json()
            self.access_token = res.get("access_token")
            return self.access_token
        else:
            print("Error al obtener el token:", response.text)
            return None

    def fetch_external_org(self):
        if not self.access_token:
            print("No se encontró un token de acceso. Intentando obtener uno...")
            if not self.get_access_token():
                return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(f"{self.api_server}/encompass/v3/externalOrganizations/tpos", headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print("Error en la consulta:", response.status_code, response.text)
            return None

    def fetch_external_companies_all(self, id):
        if not self.access_token:
            print("No se encontró un token de acceso. Intentando obtener uno...")
            if not self.get_access_token():
                return None
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(f"{self.api_server}/encompass/v3/externalOrganizations/tpos/{id}?entities=all", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print("Error en la consulta:", response.status_code, response.text)
            return None

    def fetch_external_users(self,orgId):
        if not self.access_token:
            print("No se encontró un token de acceso. Intentando obtener uno...")
            if not self.get_access_token():
                return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        response = requests.get(f"{self.api_server}/encompass/v3/externalUsers?orgId={orgId}", headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print("Error en la consulta:", response.status_code, response.text)
            return None
    def fetch_internal_users(self,orgId):
        if not self.access_token:
            print("No se encontró un token de acceso. Intentando obtener uno...")
            if not self.get_access_token():
                return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        response = requests.get(f"{self.api_server}/encompass/v3/users?orgId={orgId}&entities=all&start=0&limit=10000", headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print("Error en la consulta:", response.status_code, response.text)
            return None

    def fetch_internal_org(self):
        if not self.access_token:
            print("No se encontró un token de acceso. Intentando obtener uno...")
            if not self.get_access_token():
                return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        response = requests.get(f"{self.api_server}/encompass/v1/organizations?view=entity", headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print("Error en la consulta:", response.status_code, response.text)
            return None
    def fetch_canonical_names_loans(self):
        # /encompass/v3/schemas/loan/standardFields?start=0&limit=10000
        if not self.access_token:
            print("No se encontró un token de acceso. Intentando obtener uno...")
            if not self.get_access_token():
                return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        response = requests.get(f"{self.api_server}/encompass/v3/schemas/loan/standardFields?start=0&limit=10000", headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print("Error en la consulta:", response.status_code, response.text)
            return None
    def fetch_canonical_names_contacts(self):
        if not self.access_token:
            print("No se encontró un token de acceso. Intentando obtener uno...")
            if not self.get_access_token():
                return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(f"{self.api_server}/encompass/v1/settings/borrowerContacts/fieldDefinitions", headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            print("Error en la consulta:", response.status_code, response.text)
            return None