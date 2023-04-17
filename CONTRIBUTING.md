# Contributing to PyEnsign

**NOTE: This document is a "getting started" summary for contributing to the PyEnsign project.** Please make sure to read this page carefully to ensure the review process is as smooth as possible and to ensure the greatest likelihood of having your contribution be merged.

## How to Contribute

PyEnsign is an open source project that is supported by a community who will gratefully and humbly accept any contributions you might make to the project. Large or small, any contribution makes a big difference; and if you've never contributed to an open source project before, we hope you will start with PyEnsign!

There are many ways to contribute:

- Submit a bug report or feature request on [GitHub Issues](https://github.com/rotationalio/pyensign/issues).
- Contribute a new [example](https://github.com/rotationalio/ensign-examples/tree/main/python) that shows how to use PyEnsign.
- Assist us with [user testing](https://forms.gle/Eoh29y7fShPLYy9q7).
- Add to the documentation or help with our website, [ensign.rotational.dev](https://ensign.rotational.dev/).
- Write [unit or integration tests](https://github.com/rotationalio/pyensign/tree/main/tests) for our project.
- Answer questions on our issues, Stack Overflow, and elsewhere.
- Translate our documentation into another language.
- Write a blog post, tweet, or share our project with others.
- [Teach](https://ensign.rotational.dev/getting-started/) someone how to use PyEnsign.

As you can see, there are lots of ways to get involved and we would be very happy for you to join us! The only thing we ask is that you abide by the principles of openness, respect, and consideration of others as described in the [Python Software Foundation Code of Conduct](https://www.python.org/psf/codeofconduct/).

## Building the protocol buffers

This repo relies on [protocol buffers](https://protobuf.dev/) for code generation. If you need to rebuild the protocol buffers, clone the main ensign repo to the parent directory.

```bash
$ git clone git@github.com:rotationalio/ensign.git ../ensign
```

Then run make to build the protocol buffers from the .proto definitions.

```
$ make grpc
```

## Running the tests

As part of the local development process, you will probably want to run the tests! Make sure you first install the test dependencies (they're a bit different from the package dependencies in `requirements.txt` at the top level).

```
$ pip install -r tests/requirements.txt
```

Then run the tests using `pytest`.

```
$ python -m pytest
```

## Getting Started on GitHub

PyEnsign is hosted on GitHub at https://github.com/rotationalio/pyensign.

The typical workflow for a contributor to the codebase is as follows:

1. **Discover** a bug or a feature by using PyEnsign.
2. **Discuss** with the core contributes by [adding an issue](https://github.com/rotationalio/pyensign/issues).
3. **Fork** the repository into your own GitHub account.
4. Create a **Pull Request** first thing to [connect with us](https://github.com/rotationalio/pyensign/pulls) about your task.
5. **Code** the feature, write the documentation, add your contribution.
6. **Review** the code with core contributors who will guide you to a high quality submission.
7. **Merge** your contribution into the PyEnsign codebase.

We believe that *contribution is collaboration* and therefore emphasize *communication* throughout the open source process.

Once you have a good sense of how you are going to implement the new feature (or fix the bug!), you can reach out for feedback from the maintainers by creating a [pull request](https://github.com/rotationalio/pyensign/pulls).

Ideally, any pull request should be capable of resolution within 6 weeks of being opened. This timeline helps to keep our pull request queue small and allows PyEnsign to maintain a robust release schedule to give our users the best experience possible. However, the most important thing is to keep the dialogue going! And if you're unsure whether you can complete your idea within 6 weeks, you should still go ahead and open a PR and we will be happy to help you scope it down as needed.

If we have comments or questions when we evaluate your pull request and receive no response, we will also close the PR after this period of time. Please know that this does not mean we don't value your contribution, just that things go stale. If in the future you want to pick it back up, feel free to address our original feedback and to reference the original PR in a new pull request.

### Forking the Repository

The first step is to fork the repository into your own account. This will create a copy of the codebase that you can edit and write to. Do so by clicking the **"fork"** button in the upper right corner of the PyEnsign GitHub page.

Once forked, use the following steps to get your development environment set up on your computer:

1. Clone the repository.

    After clicking the fork button, you should be redirected to the GitHub page of the repository in your user account. You can then clone a copy of the code to your local machine.

    ```
    $ git clone https://github.com/[YOURUSERNAME]/pyensign
    $ cd pyensign
    ```

    Optionally, you can also [add the upstream remote](https://help.github.com/articles/configuring-a-remote-for-a-fork/) to synchronize with changes made by other contributors:

    ```
    $ git remote add upstream https://github.com/rotationalio/pyensign
    ```

    See "Branching Conventions" below for more on this topic.

2. Create a virtual environment.

    PyEnsign developers typically use [virtualenv](https://virtualenv.pypa.io/en/stable/) and [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/), [pyenv](https://github.com/pyenv/pyenv-virtualenv) or [conda envs](https://conda.io/docs/using/envs.html) in order to manage their Python version and dependencies. Using the virtual environment tool of your choice, create one for PyEnsign. Here's how with virtualenv:

    ```
    $ virtualenv venv
    ```

3. Install dependencies.

    PyEnsign's dependencies are in the `requirements.txt` document at the root of the repository. Open this file and uncomment the dependencies that are for development only. Then install the dependencies with `pip`:

    ```
    $ pip install -r requirements.txt
    ```

    Note that there may be other dependencies required for development and testing, you can simply install them with `pip`. For example to install
    the additional dependencies for building the documentation or to run the
    test suite, use the `requirements.txt` files in those directories:

    ```
    $ pip install -r tests/requirements.txt
    $ pip install -r docs/requirements.txt
    ```

4. Set up pre-commit hooks.

    When opening a PR in the PyEnsign repository, a series of checks will be run on your contribution, some of which lint and look at the formatting of your code. These may indicate some changes that need to be made before your contribution can be reviewed. You can set up pre-commit hooks to run these checks locally upon running `git commit` to ensure your contribution will pass formatting and linting checks. To set this up, you will need to uncomment the pre-commit line in `requirements.txt` and then run the following commands:

    ```
    $ pip install -r requirements.txt
    $ pre-commit install
    ```

    The next time you run `git commit` in the PyEnsign repository, the checks will automatically run.

5. Switch to the develop branch.

    The PyEnsign repository has a `develop` branch that is the primary working branch for contributions. It is probably already the branch you're on, but you can make sure and switch to it as follows::

    ```
    $ git fetch
    $ git checkout develop
    ```

At this point you're ready to get started writing code!

### Branching Conventions

The PyEnsign repository is set up in a typical production/release/development cycle as described in "[A Successful Git Branching Model](http://nvie.com/posts/a-successful-git-branching-model/)." The primary working branch is the `develop` branch. This should be the branch that you are working on and from, since this has all the latest code. The `master` branch contains the latest stable version and release, _which is pushed to PyPI_. No one but maintainers will push to master.

**NOTE:** All pull requests should be into the `pyensign/develop` branch from your forked repository.

You should work directly in your fork and create a pull request from your fork's develop branch into ours. We also recommend setting up an `upstream` remote so that you can easily pull the latest development changes from the main PyEnsign repository (see [configuring a remote for a fork](https://help.github.com/articles/configuring-a-remote-for-a-fork/)). You can do that as follows:

```
$ git remote add upstream https://github.com/rotationalio/pyensign.git
$ git remote -v
origin    https://github.com/YOUR_USERNAME/YOUR_FORK.git (fetch)
origin    https://github.com/YOUR_USERNAME/YOUR_FORK.git (push)
upstream  https://github.com/rotationalio/pyensign.git (fetch)
upstream  https://github.com/rotationalio/pyensign.git (push)
```

When you're ready, request a code review for your pull request. Then, when reviewed and approved, you can merge your fork into our main branch. Make sure to use the "Squash and Merge" option in order to create a Git history that is understandable.

**NOTE to maintainers**: When merging a pull request, use the "squash and merge" option and make sure to edit the both the subject and the body of the commit message so that when we're putting the changelog together, we know what happened in the PR. I recommend reading [Chris Beams' _How to Write a Git Commit Message_](https://chris.beams.io/posts/git-commit/) so we're all on the same page!

Core contributors and those who are planning on contributing multiple PRs might want to consider using feature branches to reduce the number of merges (and merge conflicts). Create a feature branch as follows:

```
$ git checkout -b feature-myfeature develop
$ git push --set-upstream origin feature-myfeature
```

Once you are done working (and everything is tested) you can submit a PR from your feature branch. Synchronize with `upstream` once the PR has been merged and delete the feature branch:

```
$ git checkout develop
$ git pull upstream develop
$ git push origin develop
$ git branch -d feature-myfeature
$ git push origin --delete feature-myfeature
```

Head back to Github and checkout another issue!