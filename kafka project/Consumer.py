from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import matplotlib.animation as animation


consumer = KafkaConsumer('Cricker_match',bootstrap_servers='localhost:9092')
MaxPrevKey=0
for message in consumer:
 if(int(message.key.decode('utf-8'))>MaxPrevKey):
   MaxPrevKey=int(message.key.decode('utf-8'))
 else:
 	break
score1list=[]
PredScore=[]
for i in range(0,MaxPrevKey+1):
 score1list.append([])
 PredScore.append([])
 
for message in consumer:
 team1,score1,team2,score2=message.value.decode('utf-8').split('@')
 counter=0
 try:
     score1=score1.split('/')
     wicket=int(score1[1].split()[0])
     overs=score1[1].split()[1].strip('(').split('.')
     balls=int(overs.split('.')[0])*6+int(overs.split('.')[1])
     sum=0
     for m in range(0,wicket):
         sum+=(10-m)
     per=(10-wicket)/sum
     StrikeRate=(int(score1[0])*per)/(balls/(wickets+1)
     if counter<8:
         score1list[int(message.key.decode('utf-8'))].append(int(score1[0])
         PredScore[int(message.key.decode('utf-8'))].append(score1[0]+StrikeRate*(300-balls))
     else:
         score1list[int(message.key.decode('utf-8'))]=score1list[int(message.key.decode('utf-8'))][1:8].append(int(score1[0])
         PredScore[int(message.key.decode('utf-8'))]=PredScore[int(message.key.decode('utf-8'))][1:8].append(score1[0]+StrikeRate*(300-balls))
 except:
     pass
for i in range(0,MaxPrevKey+1):
    print(PredScore[i],score1list[i])