# Contributing to minibatch

We love your input! We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## We Develop with Github
We use github to host code, to track issues and feature requests, as well as accept pull requests.

## We Use [Github Flow](https://guides.github.com/introduction/flow/index.html), So All Code Changes Happen Through Pull Requests
Pull requests are the best way to propose changes to the codebase (we use [Github Flow](https://guides.github.com/introduction/flow/index.html)). We actively welcome your pull requests:

1. Fork the repo and create your branch from `master`.
2. If you've added code that should be tested, add tests (in `tests/test_<part>.py`).
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes (run `$ make test`).
5. Make sure your code lints (run `$ make lint`).
6. Issue that pull request!

## Any contributions you make will be under the Apache 2.0 Software License
In short, when you submit code changes, your submissions are understood to be under the same [Appache 2.0](https://choosealicense.com/licenses/apache-2.0/) + "No Sell, Consulting Yes" License Condition that covers the project. Feel free to contact the maintainers if that's a concern.

## Report bugs using Github's [issues](https://github.com/omegaml/minibatch/issues)
We use GitHub issues to track public bugs. Report a bug by [opening a new issue](https://github.com/omegaml/minibatch/issues/new); it's that easy!

## Write bug reports with detail, background, and sample code

When submitting a bug report, please use this format

* Summary - in just one sentence describe what this issue is about
* Actual results/bug - what was the observed result    
* Expected results - what was the expected result
* Steps to reproduce - a list of steps to reproduce the problem
* Software versions - include the output of `pip freeze | grep -E "minibatch|mongo|kafka|mqtt|omega"` 

**Great Bug Reports** should include:

- A quick summary and/or background
- Steps to reproduce
  - Be specific!
  - Give sample code if you can
- What you expected would happen
- What actually happens
- Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

People *love* thorough bug reports. I'm not even kidding. Read more about [why you should care to write a good bug report](https://medium.com/pitch-perfect/how-to-write-the-perfect-bug-report-6430f5a45cd) by Haje Jan Kamps.
 

## Coding Style

In general we're following [pep8](https://www.python.org/dev/peps/pep-0008/) and use flake8 to achieve consistency.
Run the following command to see if your code matches this project's standard.
 
```
$ make lint
CONGRATULATIONS all is OK
```

## License
By contributing, you agree that your contributions will be licensed under the project's license as detailed in the LICENSE and LICENSE-NOSELLCLAUSE files

## References
This document was adapted from the open-source contribution guidelines for [Facebook's Draft](https://github.com/facebook/draft-js/blob/a9316a723f9e918afde44dea68b5f9f39b7d9b00/CONTRIBUTING.md)
