# -*- coding: utf-8 -*-
"""
Created on Sun Sep 23 02:20:32 2018

@author: Shivam  Tiwari
"""

import matplotlib.pyplot as plt
import matplotlib.animation as animate
fig=plt.figure()
axs=fig.add_subplot(1,1,1)

def FuncAnime(i):
    PredData=open('Desktop/plist.txt','r').read()
    x=[]
    y=[]
    lines=PredData.split('\n')
    for line in lines:
        x.append(line.split(' ')[1])
        y.append(line.split(' ')[0])
    axs.clear()
    axs.plot(x,y)
ani=animate.FuncAnimation(fig,FuncAnime,interval=10000)
plt.show()