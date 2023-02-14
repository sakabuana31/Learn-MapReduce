# import lib
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.runner import MRJobRunner
import re

# mengambil pattern word dari input file
WORD_RE = re.compile(r"[\w']+")

# membuat kelas
class MRWordFreqCount(MRJob):
    # mendefenisikan fungsi yang digunakan pada mrstep method
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]
    # mendefenisikan fungsi mapper
    def mapper(self, _, line):
        # melakukan ekstraksi data dengan list of string sebagai outputnya 
        for word in WORD_RE.findall(line):
            # melakukan iterasi data dari list yang telah dibuat
            yield (word.lower(), 1)
            

    # mengkombinasikan hasil iterasi data
    def combiner(self, word, counts):
        # melakukan iterasi dan menghitung jumlah kata yang sama
        yield (word, sum(counts))

    # melakukan reduce output dari combiner
    def reducer(self, word, counts):
        # melakukan iterasi yang sama dari hasil combiner yang telah direduce
        yield (word, sum(counts))

# melakukan fungsi eksekusi pengembangan script tanpa mempengaruhi modul lain
# menjalankan tes atau debug script
if __name__ == '__main__':
    # mendefenisikan lokasi input data
    input_data = "benda.txt"
    # membuat objek mr_job dari kelas MRWordFreqCount dengan argumen input data
    mr_job = MRWordFreqCount(args=[input_data])
    # menjalankan mr_job dengan lib runner
    with mr_job.make_runner() as runner:
        runner.run()
        # mendefinisikan hasil dari mr_job
        results = list(mr_job.parse_output(runner.cat_output()))
        # melakukan sorting dari mr_job dengan nilai tertinggi
        results.sort(key=lambda x: x[1], reverse=True)
        # menyimpan output mr_job dalam bentuk .txt
        with open("output.txt", "w") as f:
            for key, value in results:
                f.write(f"{key} {value}\n")
        # menampilkan output pada terminal
                print(key, value)