import pyreadstat
import pathlib

from tap_keyingress_spss.spss import QuestionCollector

def test_spss_questions():
    path = pathlib.Path(__file__).parent.joinpath('data')
    df, meta = pyreadstat.read_sav(path.joinpath("21_A_07_Clevis.sav"))
    qc = QuestionCollector(df=df, meta=meta)
    questions = qc.getQuestions()
    print(questions)
    options = qc.getOptions()
    print(options)