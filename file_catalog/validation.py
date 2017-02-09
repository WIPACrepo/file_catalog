
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
        if not set(self.config.get_list('metadata', 'mandatory_fields')).issubset(metadata):
            # check metadata for mandatory fields
            apihandler.send_error(400, reason='mandatory metadata missing (mandatory fields: %s)' % self.config['metadata']['mandatory_fields'],
                            file=apihandler.files_url)
            return False
        if not self.is_valid_sha512(metadata['checksum']):
            # force to use SHA512
            apihandler.send_error(400, reason='`checksum` needs to be a SHA512 hash',
                            file=apihandler.files_url)
            return False
        elif not isinstance(metadata['locations'], list):
            # locations needs to be a list
            apihandler.send_error(400, reason='member `locations` must be a list',
                            file=apihandler.files_url)
            return False
        elif not metadata['locations']:
            # location needs have at least one entry
            apihandler.send_error(400, reason='member `locations` must be a list with at least one url',
                            file=apihandler.files_url)
            return False
        elif not all(l for l in metadata['locations']):
            # locations aren't allowed to be empty
            apihandler.send_error(400, reason='member `locations` must be a list with at least one non-empty url',
                            file=apihandler.files_url)
            return False

        return True
