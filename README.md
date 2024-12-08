# justiceandperformance


#Este script esta em processo de construção. Passo a passo da manipulaçao de dados dos resultados do meu PhD. 





setwd("C:/Users/Erick/OneDrive/Arquivos Computador(C) Joao/DOUTORADO UFJF/0- Tese/inputs/Resultsdefesa")


# Instale o pacote basedosdados
install.packages("basedosdados")
library(basedosdados)


# Defina o ID de cobrança do Google Cloud
basedosdados::set_billing_id("projeto-teste-440917") ##Esse id é gerado la no google cloud


# Defina sua consulta SQL
query <- "
SELECT
    dados.ano as ano,
    dados.transmissao as transmissao,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.acessos as acessos,
    dados.velocidade as velocidade,
    dados.cnpj as cnpj,
    dados.produto as produto,
    dados.tecnologia as tecnologia,
    dados.sigla_uf AS sigla_uf,
    diretorio_sigla_uf.nome AS sigla_uf_nome
FROM `basedosdados.br_anatel_banda_larga_fixa.microdados` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN (SELECT DISTINCT sigla,nome  FROM `basedosdados.br_bd_diretorios_brasil.uf`) AS diretorio_sigla_uf
    ON dados.sigla_uf = diretorio_sigla_uf.sigla
"

# Baixando os dados com a função read_sql do pacote basedosdados
dadosinternetbr <- basedosdados::read_sql(query)



# Instale o pacote haven, se ainda não tiver feito isso
install.packages("haven")
install.packages("dplyr")
install.packages("stringi")
# Carregue o pacote haven
library(haven)
library(dplyr)
library(stringi)


# Renomear variáveis e filtrar apenas para o estado de MG
dadosinternetbr <- dadosinternetbr %>%
  rename(
    codigoibge = id_municipio,
    municipio = id_municipio_nome,
    uf = sigla_uf,
    estado = sigla_uf_nome
  ) %>%
  filter(uf == "MG")

# Filtrar a base de dados para os anos de 2015 a 2021
dadosinternetbr <- dadosinternetbr %>%
  filter(ano >= 2015 & ano <= 2021)

# Criando as novas variáveis com base nas informações originais
dadosinternetbr <- dadosinternetbr %>%
  mutate(
    ano1 = as.numeric(ano),
    transmissao1 = as.character(transmissao),
    codigoibge1 = as.numeric(codigoibge),
    acessos1 = as.numeric(acessos),
    municipio1 = as.character(municipio),
    velocidade1 = as.character(velocidade),
    cnpj1 = as.character(cnpj),
    produto1 = as.character(produto),
    tecnologia1 = as.character(tecnologia),
    uf1 = as.character(uf),
    estado1 = as.character(estado)
  )

# Carregar o pacote stringi, se ainda não estiver carregado
library(stringi)

# Padronizar as variáveis de texto: minúsculas, sem acentuação e sem espaços
dadosinternetbr <- dadosinternetbr %>%
  mutate(
    transmissao1 = stri_trans_general(tolower(trimws(transmissao1)), "Latin-ASCII"),
    municipio1 = stri_trans_general(tolower(trimws(municipio1)), "Latin-ASCII"),
    velocidade1 = stri_trans_general(tolower(trimws(velocidade1)), "Latin-ASCII"),
    cnpj1 = stri_trans_general(tolower(trimws(cnpj1)), "Latin-ASCII"),
    produto1 = stri_trans_general(tolower(trimws(produto1)), "Latin-ASCII"),
    tecnologia1 = stri_trans_general(tolower(trimws(tecnologia1)), "Latin-ASCII"),
    uf1 = stri_trans_general(tolower(trimws(uf1)), "Latin-ASCII"),
    estado1 = stri_trans_general(tolower(trimws(estado1)), "Latin-ASCII")
  )





# Salvando a base de dados como um arquivo .RDS
saveRDS(dadosinternetbr, "dadosinternetmg.rds")
#Salvando a base de dados no formato dta
write_dta(dadosinternetbr, "internet_mg.dta")


##########################################################
# Abrindo base para minas 
########################################################
#Pacotes
library(haven)
library(dplyr)
library(stringi)

dadosinternetmg <- readRDS("dadosinternetmg.rds")


# Remover variáveis originais
dadosinternetmg <- dadosinternetmg %>%
  select(
    ano1,
    transmissao1,
    codigoibge1,
    acessos1,
    municipio1,
    velocidade1,
    cnpj1,
    produto1,
    tecnologia1,
    uf1,
    estado1
  )


# Ver os valores únicos da variável 'municipio'
unique(dadosinternetmg$municipio1) ##Quantidade certa de municipios [853]


# Contar a frequência de cada valor em 'transmissao'
table(dadosinternetmg$transmissao1)

# Ver os valores únicos da variável 'velocidade'
unique(dadosinternetmg$velocidade1)

#[1] "0kbps a 512kbps" "512kbps a 2mbps" "2mbps a 12mbps"  "12mbps a 34mbps"
#[5] "> 34mbps"


# Contar o número de NAs em cada variável por ano
dadosinternetmg %>%
  group_by(ano1) %>%
  summarise(
    n_na_transmissao = sum(is.na(transmissao1)),
    n_na_velocidade = sum(is.na(velocidade1)),
    n_na_acessos = sum(is.na(acessos1))
  )

#A variavel velocidade nao apresenta nenhuma NA. É possível perceber que as
#variaveis de transmissao e acessos apresentam os mesmos valores
#nos respectivos anos que tem NA. Logo, se apagar o NA de uma, automaticamente
#estará apagando da outra. VOu apagar os NA e observar se o numero de municipios
#se mantém. Acredito que os NA é prestadora que nao tem no municipio, enquanto em outros municipio
#tem outra prestadora e outros serviços

dadosinternetmg <- dadosinternetmg %>%
  filter(!is.na(transmissao1) & !is.na(acessos1))
#Observando o numero de municipios:
unique(dadosinternetmg$municipio1) #[853] ok. Como esperado. 


#########################################################
# Tirando média antes de manipulaçoes para conferência
#########################################################

library(dplyr)

#Media de acessos por tipo de transmissao, a cada ano do municipio:
media_acessos_sem_collapse <- dadosinternetmg %>%
  group_by(municipio1, ano1) %>% 
  summarise(media_acessos = mean(acessos1, na.rm = TRUE))

# Ver as primeiras linhas do resultado
View(media_acessos_sem_collapse)

#Criando média e variaveis de velocidade media:


# Criar um mapeamento para converter categorias de velocidade em valores numéricos médios
mapa_velocidade <- c(
  "0kbps a 512kbps" = 256,       # Média de 0 a 512 Kbps
  "512kbps a 2mbps" = 1256,      # Média de 512 Kbps a 2 Mbps
  "2mbps a 12mbps" = 7000,       # Média de 2 Mbps a 12 Mbps
  "12mbps a 34mbps" = 23000,     # Média de 12 Mbps a 34 Mbps
  "> 34mbps" = 34000             # Mínimo de 34 Mbps
)

# Substituir categorias de velocidade por valores numéricos
dadosinternetmg <- dadosinternetmg %>%
  mutate(velocidade_num = mapa_velocidade[velocidade1])

# Calcular a média de velocidade por município e ano
mediavelocidade <- dadosinternetmg %>%
  group_by(municipio1, ano1) %>%
  summarise(
    media_velocidade = mean(velocidade_num, na.rm = TRUE), # Calcula a média ignorando valores ausentes
    .groups = "drop"
  )

# Visualizar os resultados
View(mediavelocidade)

# Criar dummies para cada combinação de velocidade1 
dadosinternetmg <- dadosinternetmg %>%
  mutate(
    dummy_0kbps_512kbps = ifelse(velocidade1 == "0kbps a 512kbps", 1, 0),
    dummy_512kbps_2mbps = ifelse(velocidade1 == "512kbps a 2mbps", 1, 0),
    dummy_2mbps_12mbps  = ifelse(velocidade1 == "2mbps a 12mbps", 1, 0),
    dummy_12mbps_34mbps = ifelse(velocidade1 == "12mbps a 34mbps", 1, 0),
    dummy_maior_34mbps  = ifelse(velocidade1 == "> 34mbps", 1, 0)
  )


# Collapsar considerando a média de 'acessos1' e velocidade_num para todos os tipos de transmissão
dados_colapsados <- dadosinternetmg %>%
  group_by(municipio1, ano1, codigoibge1) %>%
  summarise(
    media_acessos1 = mean(acessos1, na.rm = TRUE),
    media_velocidade = mean(velocidade_num, na.rm = TRUE),
    # Criando as variáveis dummy com base na variável velocidade_num
    dummy_0kbps_512kbps = as.integer(any(velocidade_num >= 0 & velocidade_num <= 512, na.rm = TRUE)),
    dummy_512kbps_2mbps = as.integer(any(velocidade_num > 512 & velocidade_num <= 2000, na.rm = TRUE)),
    dummy_2mbps_12mbps = as.integer(any(velocidade_num > 2000 & velocidade_num <= 12000, na.rm = TRUE)),
    dummy_12mbps_34mbps = as.integer(any(velocidade_num > 12000 & velocidade_num <= 34000, na.rm = TRUE)),
    dummy_maior_34mbps = as.integer(any(velocidade_num > 34000, na.rm = TRUE)),
    cnpj1 = first(cnpj1),
    uf1 = first(uf1),
    tecnologia1 = first(tecnologia1)
  ) %>%
  ungroup()


# Visualizar os dados colapsados
View(dados_colapsados)

#O collapse ocorreu bem. Analisando a média de acessos e a media de 
#velocidade por tipo de transmissao sao os mesmo valores sem o collapse.
#O que era de se esperar. Agora temos a base no nivel municipal. Um painel
#sem duplicados.


#Verificando se ha municipios duplicados

unique(dados_colapsados$municipio1)

#renomeando variaveis 
dados_colapsados <- dados_colapsados %>%
  rename(
    municipio = municipio1,
    ano = ano1,
    codigoibge = codigoibge1,
    cnpj = cnpj1,
    uf= uf1,
    tecnologia = tecnologia1,
    media_acessos = media_acessos1
  )


# Salvando a base de dados como um arquivo .RDS
saveRDS(dados_colapsados, "dados_colapsados.rds")
#Salvando a base de dados no formato dta
write_dta(dados_colapsados, "dados_colapsados.dta")


###################################################
#merged com a base criminal
######################################################
dados_colapsados <- readRDS("dados_colapsados.rds")
View(dados_colapsados)
basecriminal <- read_dta("basecriminal.dta")


library(dplyr)

# Contando o número de municípios únicos da basecriminal
num_municipios_unicos <- basecriminal %>%
  summarise(num_municipios = n_distinct(municipio))

# Exibindo o resultado
num_municipios_unicos


# Realizar o merge entre as bases usando as variáveis chave
criminal_internet <- merge(dados_colapsados, basecriminal, 
                        by = c("codigoibge", "ano", "municipio"), 
                        all = TRUE)  # all = TRUE inclui todos os registros de ambas as bases


head(criminal_internet)


### Apagando municipios que nao nos interessa (dado que estou so trabalhando com tribunais criminais)

library(dplyr)

# Filtra o dataframe diretamente, removendo as observações onde 'codigo' é NA
criminal_internet <- criminal_internet %>%
  filter(!is.na(codigo))  #observe que  ficamos com a quantidade de observaçao da base original (basecriminal). Era esperado


# Salvando a base de dados como um arquivo .RDS
saveRDS(criminal_internet, "criminal_internet.rds")
#Salvando a base de dados no formato dta
write_dta(criminal_internet, "criminal_internet.dta")


##################################################
#Realizando merge (com latitude e longitude)
##################################################


#Ativando bibliotecas
library(ggplot2)
library(maps)
library(rnaturalearth)
library(rnaturalearthhires)
library(ggplot2)
library(sf)
library(rnaturalearth)
library(rnaturalearthdata)
library(dplyr)
library(stringr)
library(haven)
library(readxl)

#Importando base de dados pje_criminal com as latitudes e longitudes das varas:
base_pje_criminal <- read_dta("base_pje_criminal.dta")
#renomeando as variaveis para o merge ocorrer bem:
base_pje_criminal <- base_pje_criminal %>%
  rename(
    municipio = `munici`,
    ano = `ano_criminal`
    
  )


# Adiciona uma coluna de identificação em ambas as bases originais
cri_inter_portipotransmissao_merge$source <- "base_original_1"
base_pje_criminal$source <- "base_original_2"


#Verificar o Total de Observações Antes e Após o Merge
cat("Observações antes do merge:\n")
cat("Base principal (cri_inter_portipotransmissao_merge):", nrow(cri_inter_portipotransmissao_merge), "\n")
cat("Base adicional (base_pje_criminal):", nrow(base_pje_criminal), "\n")
cat("Base resultante (basecriminalfinal):", nrow(basecriminalfinal), "\n")


# Observações que estão apenas na base_pje_criminal
observacoes_extras_pje <- basecriminalfinal %>%
  filter(is.na(source.x) & !is.na(source.y))

# Observações que estão apenas na base cri_inter_portipotransmissao_merge
observacoes_extras_cri <- basecriminalfinal %>%
  filter(!is.na(source.x) & is.na(source.y))

# Verificar quantidade de observações extras
cat("Observações adicionais da base_pje_criminal:", nrow(observacoes_extras_pje), "\n")
cat("Observações adicionais da cri_inter_portipotransmissao_merge:", nrow(observacoes_extras_cri), "\n")


# Realiza o merge com a opção all = TRUE
basecriminalfinal <- merge(cri_inter_portipotransmissao_merge, base_pje_criminal, 
                           by = c("codigo", "ano", "municipio"), 
                           all = TRUE) #usando true voce força o R colocar observaçoes que nao esta em um dos bancos de dados. Opcional.

# Filtrar observações que estão na base final, mas não na base original 1
novas_observacoes <- basecriminalfinal[!(basecriminalfinal$codigo %in% cri_inter_portipotransmissao_merge$codigo & 
                                           basecriminalfinal$ano %in% cri_inter_portipotransmissao_merge$ano & 
                                           basecriminalfinal$municipio %in% cri_inter_portipotransmissao_merge$municipio), ]




# Preencher os valores de latitude e longitude para todos os anos de cada tribunal (codigo)
basecriminalfinal <- basecriminalfinal %>%
  group_by(codigo) %>% # Agrupa por tribunal
  mutate(
    latitude = first(na.omit(latitude)), # Pega o primeiro valor não nulo de latitude
    longitude = first(na.omit(longitude)) # Pega o primeiro valor não nulo de longitude
  ) %>%
  ungroup()



#Observando quem sao as observaçoes a mais:


# Observações que vieram exclusivamente de base_pje_criminal
observacoes_extras_base_pje <- basecriminalfinal %>%
  filter(is.na(source.x)) # Apenas na base base_pje_criminal

# Observações que vieram exclusivamente de cri_inter_portipotransmissao_merge
observacoes_extras_cri_inter <- basecriminalfinal %>%
  filter(is.na(source.y)) # Apenas na base cri_inter_portipotransmissao_merge

# Verificar número de observações extras em cada caso
cat("Observações adicionadas da base_pje_criminal:", nrow(observacoes_extras_base_pje), "\n")
cat("Observações adicionadas da cri_inter_portipotransmissao_merge:", nrow(observacoes_extras_cri_inter), "\n")

library(dplyr)

# Criar uma coluna indicando se a observação é adicional (TRUE para as extras)
basecriminalfinal <- basecriminalfinal %>%
  mutate(is_extra = is.na(source.x) | is.na(source.y)) %>%
  arrange(desc(is_extra)) %>% # Ordena colocando as extras no topo
  select(-is_extra) # Remove a coluna auxiliar após a ordenação


#Apagando observaçoes adicionais:

# Filtrar as observações que aparecem em ambas as bases
basecriminalfinal_sem_extras <- basecriminalfinal %>%
  filter(!is.na(source.x) & !is.na(source.y))

# Verificar a quantidade de observações após a remoção
cat("Número de observações após remover as 47 extras:", nrow(basecriminalfinal_sem_extras), "\n")




#Salvando arquivo:

saveRDS(basecriminalfinal, "basecriminalfinal.rds")

