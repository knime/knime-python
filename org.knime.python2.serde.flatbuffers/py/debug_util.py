#Module encapsulating functionality used for debugging.
#See: http://www.pydev.org/manual_adv_remote_debugger.html on how to setup
#debugging.

REMOTE_DBG = False

def init_debug():
    global REMOTE_DBG
    #Debugging already enabled ? -> nothing to do here
    if REMOTE_DBG:
        return
    try:
        # for infos see http://pydev.org/manual_adv_remote_debugger.html
        # you have to create a new environment variable PYTHONPATH that points to the psrc folder
        # located in ECLIPSE\plugins\org.python.pydev_xxx
        import pydevd  # with the addon script.module.pydevd, only use `import pydevd`

        # stdoutToServer and stderrToServer redirect stdout and stderr to eclipse console
        pydevd.settrace('localhost', port=5678, suspend=False, stdoutToServer=True, stderrToServer=True)
        REMOTE_DBG = True
    except ImportError as e:
        sys.stderr.write("Error: " +
                         "You must add org.python.pydev.debug.pysrc to your PYTHONPATH. ".format(e))
        pydevd = None
        sys.exit(1)
        
def debug_msg(msg):
    global REMOTE_DBG
    if REMOTE_DBG:
        print(msg)
    
def breakpoint():
    import pydevd
    pydevd.settrace('localhost', port=5678, suspend=True, stdoutToServer=True, stderrToServer=True)
    
def is_debug_enabled():
    global REMOTE_DBG
    return REMOTE_DBG