
import re

class Validation:
    def __init__(self, config):
        self.config = config

    def is_valid_sha512(self, hash_str):
        """Checks if `hash_str` is a valid SHA512 hash"""
        return re.match(r"[0-9a-f]{128}", str(hash_str), re.IGNORECASE) is not None

    def has_forbidden_attributes_creation(self, apihandler, metadata, old_metadata):
        """
        Checks if dict (`metadata`) has forbidden attributes and they have changed.

        Returns `True` if it has forbidden attributes.
        """
        for key in set(self.config.get_list('metadata', 'forbidden_fields_creation')).intersection(metadata):
            if key not in old_metadata or metadata[key] != old_metadata[key]:
                # forbidden fields
                apihandler.send_error(400, reason='forbidden attributes',
                                      file=apihandler.files_url)
                return True
        return False

    def has_forbidden_attributes_modification(self, apihandler, metadata, old_metadata):
        """
        Same as `has_forbidden_attributes_creation()` but it has additional forbidden attributes.
        """
        for key in set(self.config.get_list('metadata', 'forbidden_fields_update')).intersection(metadata):
            if key not in old_metadata or metadata[key] != old_metadata[key]:
                # forbidden fields
                apihandler.send_error(400, reason='forbidden attributes',
                                      file=apihandler.files_url)
                return True
        return False

    def validate_metadata_creation(self, apihandler, metadata):
        """
        Validates metadata for creation

        Utilizes `send_error` and returnes `False` if validation failed.
        If validation was successful, `True` is returned.
        """
        if self.has_forbidden_attributes_creation(apihandler, metadata, {}):
            return False
        return self.validate_metadata_modification(apihandler, metadata)

    def validate_metadata_modification(self, apihandler, metadata):
        """
        Validates metadata for modification

        Utilizes `send_error` and returnes `False` if validation failed.
        If validation was successful, `True` is returned.
        """
        for field in self.config.get_list('metadata', 'mandatory_fields'):
            # check metadata for mandatory fields
            if '.' in field:
                m = metadata
                for p in field.split('.'):
                    if p not in m:
                        apihandler.send_error(400, reason='mandatory metadata missing (mandatory fields: %s)' % self.config['metadata']['mandatory_fields'],
                                        file=apihandler.files_url)
                        return False
                    m = m[p]
            elif field not in metadata:
                apihandler.send_error(400, reason='mandatory metadata missing (mandatory fields: %s)' % self.config['metadata']['mandatory_fields'],
                                file=apihandler.files_url)
                return False
        if ((not isinstance(metadata['checksum'], dict))
            or 'sha512' not in metadata['checksum']):
            # checksum needs to be a dict with an sha512
            apihandler.send_error(400, reason='member `checksum` must be a dict with a sha512 hash',
                            file=apihandler.files_url)
            return False
        elif not self.is_valid_sha512(metadata['checksum']['sha512']):
            # force to use SHA512
            apihandler.send_error(400, reason='`checksum[sha512]` needs to be a SHA512 hash',
                            file=apihandler.files_url)
            return False
        elif ((not isinstance(metadata['locations'], list))
              or (not metadata['locations'])
              or not all(l for l in metadata['locations'])):
            # locations needs to be a non-empty list
            apihandler.send_error(400, reason='member `locations` must be a list with at least one entry',
                            file=apihandler.files_url)
            return False

        return True
