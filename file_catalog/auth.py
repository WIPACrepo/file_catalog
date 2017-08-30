
import time
import jwt

class Auth:
    def __init__(self, config):
        self.secret = config['auth']['secret']
        self.apps = config['auth']['keys']
        self.app_keys = {}
        for a in self.apps:
            self.app_keys[self.apps[a]] = a
        self.max_exp = config['auth']['expiration']

    def new_key(self, app_key, expiration=None):
        """Create a new auth key.

        Args:
            app_key (str): a valid app key
            expiration (int): expiration in seconds

        Returns:
            str: auth key

        Raises:
            Exception: if app key is invalid
        """
        if app_key in self.app_keys:
            exp = self.max_exp
            if expiration and exp > expiration:
                exp = expiration
            now = time.time()
            payload = {
                'iss': self.app_keys[app_key],
                'exp': now+exp,
                'iat': now,
            }
            return jwt.encode(payload, self.secret, algorithm='HS512')
        raise Exception('failed to create auth key')

    def authorize(self, auth_key):
        """Authorize an existing auth key.

        Args:
            auth_key (str): an auth key

        Returns:
            dict: {iss, exp, iat} if valid

        Raises:
            Exception: if app key is invalid
        """
        return jwt.decode(auth_key, self.secret, algorithms=['HS512'])
