# PyEnsign Documentation

*Welcome to the PyEnsign docs!*

## reStructuredText

PyEnsign uses [Sphinx](http://www.sphinx-doc.org/en/master/index.html) to build our documentation. With Sphinx, docstrings used to describe PyEnsign functions can be automatically included when the documentation is built via [autodoc](http://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#sphinx.ext.autodoc).

To take advantage of these features, our documentation must be written in reStructuredText (or "rst"). reStructuredText is similar to markdown, but not identical, and does take some getting used to. For instance, styling for things like codeblocks, external hyperlinks, internal cross references, notes, and fixed-width text are all unique in rst.

If you would like to contribute to our documentation and do not have prior experience with rst, we recommend you make use of these resources:

- [A reStructuredText Primer](http://docutils.sourceforge.net/docs/user/rst/quickstart.html)
- [rst notes and cheatsheet](https://cheat.readthedocs.io/en/latest/rst.html)

*Note: If you are on an M1 mac and you want to [install sphinx using brew](https://www.sphinx-doc.org/en/master/usage/installation.html#homebrew), make sure you have the [correct brew installation](https://stackoverflow.com/a/64997047).


## Building the Docs

To build the documents locally, first install any documentation-specific requirements with `pip` using the `requirements.txt` file in the `docs` directory:

```bash
$ pip install -r docs/requirements.txt
```

You will then be able to build the documentation from inside the `docs` directory by running `make html`; the documentation will be built and rendered in the `_build/html` directory. You can view it by opening `_build/html/index.html` then navigating to your documentation in the browser.