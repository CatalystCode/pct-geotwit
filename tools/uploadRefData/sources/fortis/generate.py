import os
import csv
import openpyxl

#  Extract Fortis data from .xlsx file

DATA_DIR = "../../data/fortis";
INPUT_FILE = "DengueInference- Indonesia and sri lanka_may 19.xlsx";

def main():

  # Read the spreadsheet 'filename' and generate keyword and
  # translation files for bahasa indonesian and sinhalese

  id_to_en = {}
  si_to_en = {}

  id_keywords = []
  si_keywords = []

  wb = openpyxl.load_workbook(INPUT_FILE);
  keywords = wb.worksheets[0]
  for row in keywords.rows[1:]:

    en = row[1].value.strip()  # english
    _id = row[2].value.strip() # bahasa indonesian
    _si = row[4].value.strip() # sinhalese

    id_to_en[_id] = en
    id_keywords.append(_id)

    si_to_en[_si] = en
    si_keywords.append(_si)

    if row[3].value:
      synonyms = map(lambda s : s.strip(), row[3].value.split(","))
      id_keywords += synonyms
      for synonym in synonyms:
        id_to_en[synonym] = en

    if row[5].value:
      synonyms = map(lambda s : s.strip(), row[5].value.split(","))
      si_keywords += synonyms
      for synonym in synonyms:
        si_to_en[synonym] = en


  def createDataDir(d):
    _dir = os.path.join(DATA_DIR, d);
    if not os.path.exists(_dir):
      os.makedirs(_dir)
    return _dir

  _dir = createDataDir("keywords/id");
  with file(os.path.join(_dir, "keywords_id.csv"), "wb") as f:
    writer = csv.writer(f)
    writer.writerow(id_keywords)

  _dir = createDataDir("keywords/si");
  with file(os.path.join(_dir, "keywords_si.csv"), "wb") as f:
    writer = csv.writer(f)
    writer.writerow(si_keywords)

if __name__ == "__main__":
  main()
