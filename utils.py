from datetime import datetime
import re

class Utils():
    
    def linha_para_array(linha):
        
        linha = '''piweba3y.prodigy.com - - [03/Jul/1995:17:20:53 -0400] "GET /history/apollo/apollo-13/apo;;p-13.html HTTP/1.0" 404 -'''

        token_1 = '''\s-\s-\s\['''    # sequencia de caracteres entre o host e a data do log
        token_2 = '''\]\s"'''         # sequencia de caracteres entre a data do log e a metodo http acessado
        token_3 = '''"\s(?=\d{3})'''  # sequencia de caracteres entre o metodo http acessado e o http code
    
        nova_linha = re.sub(token_1, "$", linha)
        nova_linha = re.sub(token_2, "$", nova_linha)
        nova_linha = re.sub(token_3, "$", nova_linha)

        first_array = nova_linha.split("$") 
        #print (first_array)

        try:
            host = first_array[0]
            data_log = datetime.strptime(first_array[1],'%d/%b/%Y:%H:%M:%S %z')
            cod_http = first_array[3].split(" ")[0]
            
            if (first_array[3].split(" ")[1].isdigit()):
                tamanho = first_array[3].split(" ")[1]
            else:
                tamanho = 0

            linha_com_erro = ""
        except:
            host = ""
            data_log = ""
            cod_http = ""
            tamanho = ""
            linha_com_erro = linha
    
        final_array = [
            host,
            data_log,
            cod_http,
            tamanho,
            linha_com_erro
        ]

        return final_array