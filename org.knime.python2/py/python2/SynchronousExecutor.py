# -*- coding: utf-8 -*-
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
#  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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

"""
@author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
@author Christian Dietz, KNIME GmbH, Konstanz, Germany
"""

import sys


class SynchronousExecutor(object):
    """
    Dummy executor that mimics a part of the interface of Python 3 futures.ThreadPoolExecutor.
    """

    def __init__(self):
        self._shutdown = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def submit(self, fn, *args, **kwargs):
        """
        Immediately computes the given function using the given arguments. Blocks until computation is completed.
        """
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')
        future = SynchronousExecutor._ImmediatelyCompletingFuture(fn, *args, **kwargs)
        future.result()  # Immediately raise exception in case one was recorded.
        return future

    def shutdown(self, wait=True):
        self._shutdown = True

    class _ImmediatelyCompletingFuture(object):
        """
        Dummy future that mimics a part of the interface of Python 3 _base.Future.
        Immediately computes the given function using the given arguments. Blocks until computation is completed.
        """

        def __init__(self, fn, *args, **kwargs):
            self._result = None
            self._exc_info = None
            try:
                result = fn(*args, **kwargs)
            except BaseException:
                self._exc_info = sys.exc_info()
            else:
                self._result = result

        def result(self, timout=None):
            if self._exc_info:
                exc_info = self._exc_info
                raise exc_info[0], exc_info[1], exc_info[2]  # Python 2 syntax, won't work with Python 3.
            else:
                return self._result

        def exc_info(self, timeout=None):
            return self._exc_info
