# Datahub Kafka Libs Release Management

We are using an automated approach to Release Management for this repository. Our branching strategy is based on [Github Flow](https://guides.github.com/introduction/flow/). For every new feature or bug fix, create a branch from the master and implement changes there. Every push will trigger a pipeline that builds a SNAPSHOT version of the library called `<branch-name>-SNAPSHOT`. The snapshots are pushed both to [Artifactory](https://artifactory.intranet.cnb/artifactory/webapp/#/artifacts/browse/tree/General/d3a-maven) and [Nexus](https://nexus.platforms.engineering/repository/monsanto-tps-snapshots/).

Every merge to the master branch (via Merge Requests) will automatically increase the MINOR version number and tagging the commit in Gitlab. For every tagged commit a release version will automatically be built and distributed to Artifactory and Nexus after approval. 

## How do I make changes?

1. Create a branch.
2. Commit and push your changes.
3. Use the SNAPSHOT release ([Artifactory](https://artifactory.intranet.cnb/artifactory/webapp/#/artifacts/browse/tree/General/d3a-maven), [Nexus](https://nexus.platforms.engineering/repository/monsanto-tps-snapshots/)) to test your changes.
4. When your implementation looks fine, create a Merge Request to the master branch.
5. Upon approving the MR, the commit will be automatically tagged with an increased version number. The pipeline contains a manual step to publish the artifact to Artifactory and Nexus.

## How do I increase the MAJOR version?

Create a tag on the master branch with an increased MAJOR version number (e.g. 2.0). Gitlab will then automatically build a release with version 2.0 and publish the artifact after approval. From that point on, every new release will have the new MAJOR version number.

## The pom.xml still has version 1.0-SNAPSHOT. Isn't that wrong?

Versioning is built upon git tags. The pipeline always fetches the correct version number from the latest tag and replaces the one in the pom.xml. There is no need to update the version number in the pom.xml, it will not have any effect.

## How can you tag a commit from inside a CI/CD pipeline? Isn't that dangerous?

The general idea of tagging a commit from inside the pipeline is based on [this excellent blog post](https://vadosware.io/post/fun-with-gitlab-ci/). You need to authenticate to Gitlab via SSH in order to be able to push the created tag. For that, we are using a [Deploy Key](https://by-gitlab.de.bayer.cnb/datasvcs/datahub-kafka-libs/-/settings/repository) that has write access to the repository. The Deploy Key contains the public part of an SSH key. The private part has to be available on the Gitlab Runner for authentication and is stored in the [CI/CD variables](https://by-gitlab.de.bayer.cnb/datasvcs/datahub-kafka-libs/-/settings/ci_cd).

Pushing to the repository from a CI/CD pipeline can be dangerous and you should be careful about doing that, because you could end up with a pipeline that triggers itself in an endless loop. Therefore the `only/except` statements in the pipeline definition are very important! When you approve a Merge Request to the master branch, this pipeline does two things:

1. Run a pipeline that creates and pushes a tag to the repository. This runs only for commits on the master branch, but except for any tags. 
2. Run a pipeline that publishes the artifacts based on a version number in the git tag. This pipeline only runs for commits on the master branch that have a tag.

By separating these two steps into two distinct pipeline executions, we ensure that we do not end up with an endlessly running pipeline.
