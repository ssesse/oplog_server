import os
import sys
import gzip
import re


class RotatingFile(object):
    def __init__(self, directory='', filename='oplog.bson', max_file_size=128 * 1024 * 1024, rotate_by='size',
                 compress_method='gzip', write_mod='ab', flush_rt=0):
        # self.ii = 1
        self.rotate_by = rotate_by
        self.directory, self.filename = directory, filename
        self.max_file_size = max_file_size
        self.finished, self.fh = False, None
        # self.current_file_name = ""
        self.compress_method = compress_method
        self.write_mod = write_mod
        self.file_sequence = self._get_file_name_sequence()
        self.open()
        self.flush_rt = flush_rt
        # self.current_file = ""

    def rotate(self):
        """Rotate the file, if necessary"""
        if self.rotate_by == 'size':
            if os.stat(self.current_file).st_size > self.max_file_size:
                self.close()
                self.sequence_increment()
                self.open()
                # if (self.ii <= self.max_files):
                #     self.open()
                # else:
                #     self.close()
                #     self.finished = True

    def _get_file_name_sequence(self):
        file_list = os.listdir()
        x = []
        for file in file_list:
            match_obj = re.compile(
                r'{filename}_([0-9]*){file_postfix}'.format(filename=self.filename,
                                                            file_postfix=self.file_name_postfix))
            # print(int(match_obj.findall(file)[-1]))
            match_reg = match_obj.findall(file)
            if len(match_reg) == 0:
                continue
            else:
                x.append(int(match_obj.findall(file)[-1]))
        if len(x) == 0:
            return 1
        # x = [int(
        #     re.compile(r'{filename}_([0-9]*){file_postfix}'.format(filename=self.filename,
        #                                                            file_postfix=self.file_name_postfix)).findall(file)[
        #         -1]) for file in file_list]
        max_sequence = max(x)
        return max_sequence
        # if sequence_increment == 0:
        #         #     return max_sequence
        #         # else:
        #         #     return max_sequence + 1

    def open(self):
        if self.compress_method == 'gzip':
            self.fh = gzip.open(self.current_file, self.write_mod)
        else:
            self.fh = open(self.current_file, self.write_mod)

    def write(self, text=""):
        self.fh.write(text)
        if self.flush_rt == 1:
            self.fh.flush()
        self.rotate()

    def close(self):
        self.fh.close()

    @property
    def file_name_postfix(self):
        if self.compress_method == 'gzip':
            return '.gz'
        else:
            return ""

    # def get_write_file_name(self):
    #     file_name_sequence = self._get_file_name_sequence()
    #     self.current_file_name = self.filename + '_' + str(file_name_sequence) + self.file_name_postfix
    #     return self.current_file + self.file_name_postfix

    def sequence_increment(self):
        self.file_sequence += 1

    @property
    def current_file(self):
        return self.directory + '/' + self.filename + '_' + str(self.file_sequence) + self.file_name_postfix


if __name__ == '__main__':
    myfile = RotatingFile()
    while not myfile.finished:
        myfile.write('this is a test')
