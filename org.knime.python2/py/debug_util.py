
# ------------------------------------------------------------------------
#  Copyright by KNIME AG, Zurich, Switzerland
#  Website: http://www.knime.com; Email: contact@knime.com
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License, Version 3, as
#  published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses>.
#
#  Additional permission under GNU GPL version 3 section 7:
#
#  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
#  Hence, KNIME and ECLIPSE are both independent programs and are not
#  derived from each other. Should, however, the interpretation of the
#  GNU GPL Version 3 ("License") under any applicable laws result in
#  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
#  you the additional permission to use and propagate KNIME together with
#  ECLIPSE with only the license terms in place for ECLIPSE applying to
#  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
#  license terms of ECLIPSE themselves allow for the respective use and
#  propagation of ECLIPSE together with KNIME.
#
#  Additional permission relating to nodes for KNIME that extend the Node
#  Extension (and in particular that are based on subclasses of NodeModel,
#  NodeDialog, and NodeView) and that only interoperate with KNIME through
#  standard APIs ("Nodes"):
#  Nodes are deemed to be separate and independent programs and to not be
#  covered works.  Notwithstanding anything to the contrary in the
#  License, the License does not apply to Nodes, you are not required to
#  license Nodes under the License, and you are granted a license to
#  prepare and propagate Nodes, in each case even if such Nodes are
#  propagated with or for interoperation with KNIME.  The owner of a Node
#  may freely choose the license terms applicable to such Node, including
#  when such Node is propagated with or for interoperation with KNIME.
# ------------------------------------------------------------------------

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