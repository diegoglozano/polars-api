def in_notebook():
    try:
        from IPython import get_ipython

        return get_ipython() is not None and "IPKernelApp" in get_ipython().config
    except Exception:
        return False
