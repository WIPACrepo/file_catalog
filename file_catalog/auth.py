
import time
import jwt
import ldap3

ISSUER = 'file_catalog'

class Auth:
    def __init__(self, config):
        self.secret = config['auth']['secret']
        self.max_exp = config['auth'].get('expiration', 86400) # 1 day
        self.max_exp_appkey = config['auth'].get('expiration_appkey', 31622400) # 1 year
        self.ldap_uri = config['auth'].get('ldap_uri', '')
        self.ldap_base = config['auth'].get('ldap_base', '')

    def _create_jwt(self, subject, expiration=None, type='temp', payload=None):
        exp = self.max_exp if type == 'temp' else self.max_exp_appkey
        if expiration and exp > expiration:
            exp = expiration
        now = time.time()
        if not payload:
            payload = {}
        payload.update({
            'iss': ISSUER,
            'sub': subject,
            'exp': now+exp,
            'nbf': now,
            'iat': now,
            'type': type,
        })
        return jwt.encode(payload, self.secret, algorithm='HS512')

    def new_appkey_ldap(self, username, password, expiration=None):
        """Create a new appkey from LDAP login.

        Args:
            app_key (str): a valid app key
            expiration (int): expiration in seconds

        Returns:
            str: auth key

        Raises:
            Exception: if app key is invalid
        """
        try:
            conn = ldap3.Connection(self.ldap_uri, 'uid={},{}'.format(username, self.ldap_base),
                                    password, auto_bind=True)
        except:
            raise Exception('failed to create auth key')
        else:
            return self._create_jwt(username, expiration, type='appkey')

    def new_temp_key(self, app_key, expiration=None):
        """Create a new temp auth key from a registered application key.

        Args:
            app_key (str): a valid app key
            expiration (int): expiration in seconds

        Returns:
            str: auth key

        Raises:
            Exception: if app key is invalid
        """
        data = self.authorize(app_key)
        if (not expiration) and 'exp' in data:
            expiration = data['exp']
        if data['type'] == 'appkey':
            return self._create_jwt(data['sub'], expiration=expiration)
        raise Exception('failed to create auth key')

    def authorize(self, auth_key):
        """Authorize an existing auth key.

        Args:
            auth_key (str): an auth key

        Returns:
            dict: {iss, sub, exp, nbf, iat, ...} if valid

        Raises:
            Exception: if app key is invalid
        """
        ret = jwt.decode(auth_key, self.secret, issuer=ISSUER, algorithms=['HS512'])
        if 'type' not in ret:
            raise Exception('no type information')
        return ret
