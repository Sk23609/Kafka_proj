from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
from kafka import KafkaProducer


ProdObject = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
  
        cric_url='https://www.cricbuzz.com/cricket-match/live-scores'
        page=uReq(cric_url)
        html_page=page.read()
        page.close()
        souped_page=soup(html_page,"html.parser")
        containers=souped_page.findAll("div",{"class":"cb-mtch-lst cb-col cb-col-100 cb-tms-itm"})
        print("request sent")
        try:
         ct1=((containers[3].findAll("div",{"class":"cb-col"}))[1]).div.findAll("div",{"class":"cb-lv-scrs-col text-black"})[0]
         team1=ct1.findAll("span",{"class":"text-bold"})[0]
         team2=ct1.findAll("span",{"class":"text-bold"})[1]
         score1=team1.find_next_sibling(text=True)
         score2=team2.find_next_sibling(text=True)
         score1=score1.split('/')
         score2=score2.split('/')
         wicket=int(score1[1].split()[0])
         overs=score1[1].split()[1].strip('(').split('.')
         balls=int(overs[0])*6+int(overs[1])
         sum=0
         for m in range(0,wicket):
           sum+=(10-m)
         per=(10-wicket)/sum
         StrikeRate=(int(score1[0])*per)/(balls/(wicket+1))
         PredScore=int(score1[0])+StrikeRate*(300-balls)
         def PredSc(wick,ba):
              sum1=0
              for m1 in range(0,wick):
                sum+=(10-m)
              pe=(10-wick)/sum1
          StrikeRate1=(int(score1[0])*pe)/(ba/(wick+1))
          return((int(score1[0])+StrikeRate1*(300-ba))/(int(score2[0])))
         def prob(bno):
             if(bno!=10)
               return(0.5*min(1,PredSc(wicket,balls))+0.5*prob(bno+1))
              else 
               return(PredSc(9,balls))
         probwin=prob(wicket)
        except:
         pass
        KeyBytesObject=bytes(str(PredScore), encoding='utf-8')
        ValueBytesObject=bytes(str(balls), encoding='utf-8')+bytes(len(team1.text.encode('utf-8')),encoding='utf-8')+bytes(team1.text,encoding='utf-8')+bytes(len(team2.text.encode('utf-8')),encoding='utf-8')+bytes(team2.text,encoding='utf-8')+bytes(probwin,encoding='utf-8')
        print("encoded")
        ProdObject.send('Cricket_match',KeyBytesObject,ValueBytesObject)
        print("record sent")
        ProdObject.flush()
 
