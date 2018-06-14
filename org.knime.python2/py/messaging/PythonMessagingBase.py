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

import abc

from messaging.MessageDistributor import MessageDistributor


class PythonMessagingBase(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._is_running = False
        self._message_id = -1
        self._distributor = MessageDistributor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abc.abstractmethod
    def create_receive_queue(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def send(self, message):
        raise NotImplementedError()

    @abc.abstractmethod
    def handle(self, message):
        raise NotImplementedError()

    @abc.abstractmethod
    def _start(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def _close(self):
        raise NotImplementedError()

    @property
    def is_running(self):
        return self._is_running

    def create_next_message_id(self):
        """
        Returns the next unique message id.
        """
        message_id = self._message_id
        self._message_id -= 1
        return message_id

    def register_message_handler(self, message_category, handler):
        return self._distributor.register_message_handler(message_category, handler)

    def unregister_message_handler(self, message_category):
        return self._distributor.unregister_message_handler(message_category)

    def start(self):
        if not self._is_running:
            self._is_running = True
            self._start()

    def close(self):
        if self._is_running:
            self._is_running = False
            self._close()
