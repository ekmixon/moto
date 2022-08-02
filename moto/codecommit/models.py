from boto3 import Session
from moto.core import BaseBackend, BaseModel
from moto.core.utils import iso_8601_datetime_with_milliseconds
from datetime import datetime
from moto.core import ACCOUNT_ID
from .exceptions import RepositoryDoesNotExistException, RepositoryNameExistsException
import uuid


class CodeCommit(BaseModel):
    def __init__(self, region, repository_description, repository_name):
        current_date = iso_8601_datetime_with_milliseconds(datetime.utcnow())
        self.repository_metadata = {}
        self.repository_metadata["repositoryName"] = repository_name
        self.repository_metadata[
            "cloneUrlSsh"
        ] = "ssh://git-codecommit.{0}.amazonaws.com/v1/repos/{1}".format(
            region, repository_name
        )
        self.repository_metadata[
            "cloneUrlHttp"
        ] = "https://git-codecommit.{0}.amazonaws.com/v1/repos/{1}".format(
            region, repository_name
        )
        self.repository_metadata["creationDate"] = current_date
        self.repository_metadata["lastModifiedDate"] = current_date
        self.repository_metadata["repositoryDescription"] = repository_description
        self.repository_metadata["repositoryId"] = str(uuid.uuid4())
        self.repository_metadata["Arn"] = "arn:aws:codecommit:{0}:{1}:{2}".format(
            region, ACCOUNT_ID, repository_name
        )
        self.repository_metadata["accountId"] = ACCOUNT_ID


class CodeCommitBackend(BaseBackend):
    def __init__(self):
        self.repositories = {}

    @staticmethod
    def default_vpc_endpoint_service(service_region, zones):
        """Default VPC endpoint service."""
        return BaseBackend.default_vpc_endpoint_service_factory(
            service_region, zones, "codecommit"
        )

    def create_repository(self, region, repository_name, repository_description):
        if repository := self.repositories.get(repository_name):
            raise RepositoryNameExistsException(repository_name)

        self.repositories[repository_name] = CodeCommit(
            region, repository_description, repository_name
        )

        return self.repositories[repository_name].repository_metadata

    def get_repository(self, repository_name):
        if repository := self.repositories.get(repository_name):
            return repository.repository_metadata
        else:
            raise RepositoryDoesNotExistException(repository_name)

    def delete_repository(self, repository_name):
        if repository := self.repositories.get(repository_name):
            self.repositories.pop(repository_name)
            return repository.repository_metadata.get("repositoryId")

        return None


codecommit_backends = {
    region: CodeCommitBackend()
    for region in Session().get_available_regions("codecommit")
}
