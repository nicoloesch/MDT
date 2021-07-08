import sys
from PyQt5 import QtCore, QtGui, QtWidgets
from ui.UIMain import UIMain


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = UIMain()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()