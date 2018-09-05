#!flask/bin/python
from flask import Flask
from flask import request
import mysql.connector
import datetime
import csv
import statistics
from datetime import datetime

app = Flask(__name__)


@app.route('/')
def index():
    return "TCC!"


@app.route('/import')
def importData():
    importData(True)
    return "Data imported!"


@app.route('/completude')
def completude():
    start = request.args.get('start')
    if not start:
        start = 1
    end = request.args.get('end')
    if not end:
        end = 50
    dados = readData()
    validados = validCompletude(dados)
    valids = 0
    invalids = 0
    for x in range(int(start), int(end)):
        if validados[x]["completude"]:
            valids += 1
        else:
            invalids += 1
    porc = (valids * 100) / (valids + invalids)

    return str("{0:.2f}".format(round(porc, 2)))


@app.route('/precision')
def precision():
    start = request.args.get('start')
    if not start:
        start = 1
    end = request.args.get('end')
    if not end:
        end = 50
    dados = readData()
    validados = validPrecision(dados)
    final = ""
    for x in range(int(start), int(end)):
        ano = str(validados[x]["data"].year)
        mes = str(validados[x]["data"].month)
        dia = str(validados[x]["data"].day)
        hora = str(validados[x]["data"].hour)
        minuto = str(validados[x]["data"].minute)
        temperatura = str(validados[x]["temperatura"])

        if validados[x]["precisao"]:
            final += "[Date.UTC(" + ano + ", " + mes + ", " + dia + ", " + hora + ", " + minuto + "), " + temperatura + "],"
            # final += "{x: Date.UTC(" + ano + ", " + mes + ", " + dia + ", " + hora + ", " + minuto + "), y: " + temperatura + ", marker: { fillColor: '#BF0B23', radius: 2 } },"
        else:
            final += "{x: Date.UTC(" + ano + ", " + mes + ", " + dia + ", " + hora + ", " + minuto + "), y: " + temperatura + ", marker: { fillColor: '#BF0B23', radius: 4 } },"
            # final += "[Date.UTC(" + ano + ", " + mes + ", " + dia + ", " + hora + ", " + minuto + "), " + temperatura + "],"
    return final[:-1]


def importData(shouldDelete):
    cnx = mysql.connector.connect(user='root', password='root',
                                  host='127.0.0.1',
                                  database='tcc')
    cursor = cnx.cursor()
    if (shouldDelete):
        cursor.execute("DELETE FROM qualidade_dados WHERE idqualidade_dados > 0")
        cursor.execute("ALTER TABLE qualidade_dados AUTO_INCREMENT = 1")

    with open("data-ori.csv", "r") as fp:
        spamreader = csv.reader(fp, delimiter=" ")
        skip = True
        for line in spamreader:
            if skip:
                skip = False
                continue
            data = datetime.strptime(line[0] + " " + line[1], "%d/%m/%Y %H:%M")
            temperatura = float(line[2])
            umidade = float(line[22])
            query = ("INSERT INTO qualidade_dados "
                     "(data, temperatura, umidade) "
                     "VALUES (%s, %s, %s)")
            values = (data, temperatura, umidade)
            cursor.execute(query, values)

        cnx.commit()
        cursor.close()
        cnx.close()


def readData():
    cnx = mysql.connector.connect(user='root', password='root',
                                  host='127.0.0.1',
                                  database='tcc')

    cursor = cnx.cursor()

    query = ("SELECT idqualidade_dados, data, temperatura, umidade FROM qualidade_dados")

    # hire_start = datetime.date(2017, 2, 23)

    # cursor.execute(query, hire_start)
    cursor.execute(query)

    dados = {}
    for (id, data, temperatura, umidade) in cursor:
        valores = {}
        valores["data"] = data
        valores["temperatura"] = temperatura
        valores["umidade"] = umidade
        valores["precisao"] = True
        valores["completude"] = True
        # print("{}> {:%d %b %Y} - {} - {}".format(id, data, temperatura, umidade))
        dados[id] = valores

    cursor.close()
    cnx.close()

    return dados


def getDesvio(dados, anteriores):
    temps = []
    for z in anteriores:
        if dados[z]["precisao"]:
            temps.append(dados[z]["temperatura"])
    desvio = 10
    if len(temps) > 0:
        desvio = statistics.pstdev(temps)
    return desvio


def validCompletude(dados):
    for x in range(1, len(dados)):
        if not dados[x]["temperatura"] or not dados[x]["umidade"]:
            dados[x]["completude"] = False
    return dados


def validPrecision(dados):
    for x in range(1, len(dados)):
        desvio = 10
        lastValidData = dados[x]
        # print("{} = {}".format(x, dados[x]))
        if x > 10:
            anteriores = [x - y for y in range(1, 10)]
            print(anteriores)
            desvio = getDesvio(dados, anteriores) * 2.5
            print(desvio)
            lastValidData = getLastValidData(dados, x, 5)
            if abs(dados[x]["temperatura"] - lastValidData["temperatura"]) > desvio and dados[x]["precisao"] == True:
                print("Precisao invalida = {}, {}: diff {} - {} = {} -> var = {}"
                      .format(x, dados[x]["data"], dados[x]["temperatura"], lastValidData["temperatura"],
                              abs(dados[x]["temperatura"] - lastValidData["temperatura"]), desvio))
                dados[x]["precisao"] = False

        print("{} - {}: {} -> {}".format(x, dados[x], desvio, lastValidData))
    return dados


def getLastValidData(dados, x, tent):
    if x > 2 and tent > 0:
        if dados[x - 1]["precisao"]:
            return dados[x - 1]
        else:
            return getLastValidData(dados, x - 1, tent - 1)

    return dados[1]


if __name__ == '__main__':
    app.run(debug=True)
