from cliquesadmin.gce_utils import CliquesGCESettings


class CliquesComputeSettings(CliquesGCESettings):
    API_VERSION = 'v1'
    API_NAME = 'compute'
    SCOPE = 'https://www.googleapis.com/auth/compute'
    # CLIENT_SECRETS = os.path.join(REPOSITORY_PATH, 'client_secrets.json')
    # OAUTH2_STORAGE = os.path.join(REPOSITORY_PATH, 'oauth2.dat')

compute_settings = CliquesComputeSettings()
