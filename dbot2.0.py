import time
import os
import discord
import pandas as pd

import ticker4discord as td
import matplotlib as plt
import pickle
plt.use('Agg')
interactive : False
showcondor = []
condorev,l,u,exp,fresh,update = {},{},{},{},{},{}
resultdf = td.pd.DataFrame()
def setdefaults(user):
    l[user] = td.l
    u[user] = td.u
    exp[user] = 2
    fresh[user] = 1
    update[user] = 0
    try:
        os.mkdir('users/'+user)
    except:
        pass
client = discord.Client()
def changesubscriptions(newtokenslist):
    print("Should Receive ",len(newtokenslist))
    oldsub=list(td.last_tick.keys())
    td.kws.close()
    td.kws.unsubscribe(oldsub)
    td.kws.subscribe(newtokenslist)
    td.kws.set_mode(td.kws.MODE_FULL,newtokenslist)
    td.kws.connect(threaded=1)
    time.sleep(1)
def updatetradables(lstrike,ustrike):
    newtokenslist=[]
    instru=td.tradables(td.allinstruments,lstrike,ustrike)
    for ins in instru:
        newtokenslist.append(ins['instrument_token'])
    changesubscriptions(newtokenslist)


def updateparameter(txt,user):
    global l, u, exp, fresh, update
    if 'minstrike' in txt:
        l[user] = int(txt.replace('$minstrike', ''))
        return
    if 'maxstrike' in txt:
        u[user] = int(txt.replace('$maxstrike', ''))
        return
    if 'exp' in txt:
        exp[user] = int(txt.replace('$exp', ''))
        return
    if 'fresh' in txt:
        fresh[user] = int(txt.replace('$fresh', ''))
        return
    if 'update' in txt:
        update[user] = int(txt.replace('$update', ''))
        return
def uc(user):
    try:
        os.remove('users/' + user + '/resultdf' + user)
        os.remove('users/' + user + '/condorev' + user)
    except Exception as E:
        print(E)
    condev, shcondor, resltdf = td.updatecondor(l[user], u[user], exp[user], fresh[user], update[user])
    resltdf.to_pickle('users/' + user + '/resultdf' + user)
    condorevfile=open('users/'+user+'/condorev'+user,'wb')
    pickle.dump(condev,condorevfile)
    condorevfile.close()
    return len(condev)

def printplot(n,user):
    cevfile=open('users/'+user+'/condorev'+user,'rb')
    cev=pickle.load(cevfile)
    cevfile.close()
    resdf=pd.read_pickle('users/'+user+'/resultdf'+user)
    print('Plotting Condor No: ', str(n))
    keys = list(cev.keys())
    name = keys[n]
    print(name)
    pop = str(int(100*(resdf[name] >= 0).sum() / len(resdf[name])))
    contracts=name.replace('[','').replace(']','').replace('&',',').replace('-',',').replace(' ','').split(',')
    nname='['+contracts[0][-7:]+'-'+contracts[1][-7:]+']'+' & '+'['+contracts[2][-7:]+'-'+contracts[3][-7:]+']'
    resdf[nname]=resdf[name]
    pop = str(int(100 * (resdf[nname] >= 0).sum() / len(resdf[nname])))
    figure=resdf.plot(x="spot", y=nname, grid=1,label='PnL',title=nname+' POP:'+pop+'%').get_figure().savefig('plots/'+name+'.png')
    return 'plots/'+name+'.png'
def printpositionsplot(user):
    resdf=pd.read_pickle('resultdf'+user)
    print('Plotting Current Positions')
    pop = str(int(100 * (resdf['positions'] >= 0).sum() / len(resdf['positions'])))
    figure=resdf.plot(x='spot', y='positions', grid=1, label='PnL', title='Positions'+' POP:'+pop+'%').get_figure().savefig('plots/' + 'positions' + '.png')
    return 'plots/'+'positions'+'.png'

def startbot():
    client.run(td.discordtoken)

@client.event
async def on_ready():
    print('Logged in as {0.user}'.format(client))

td.thread.start_new_thread(startbot, ())
td.kws.close()
@client.event
async def on_message(message):
    global l, u, exp, fresh, update, showcondor, condorev, resultdf
    if message.author == client.user:
        return
    if str(message.author) != 'monster#3020' and str(message.author) !='monstertrade097#3406':
        print("Unauthorized Text from ", message.author)
        return
    user=str(message.author)
    if user not in l.keys():
        setdefaults(user)
    txt = message.content
    if message.content.startswith('$'):
        if 'minstrike' in txt:
            updateparameter(txt,user)
            await message.channel.send('Min Strike updated to: ' + str(l[user]))
            return
        if 'maxstrike' in txt:
            updateparameter(txt,user)
            await message.channel.send('Max Strike updated to: ' + str(u[user]))
            return
        if 'exp' in txt:
            updateparameter(txt,user)
            await message.channel.send('Days to expiry updated to: ' + str(exp[user]))
            return
        if 'fresh' in txt:
            if user!='monster#3020':
                await message.channel.send('Zerodha Not Connected. Fresh set to: ' + str(fresh[user]))
                return
            updateparameter(txt, user)
            await message.channel.send('Look for Fresh Condors updated to: ' + str(fresh[user]))
            return
        if 'update' in txt:
            if user != 'monster#3020':
                await message.channel.send('Cannot update positions. Update Positions set to: ' + str(fresh[user]))
                return
            updateparameter(txt, user)
            await message.channel.send('Check for new positions updated to: ' + str(update[user]))
            return
        if txt=='$showparams':
            await message.channel.send(
                '[{0} {1}] {2} Days Fresh={3} UpdatePositions={4}'.format(str(l[user]), str(u[user]), str(exp[user]),str(fresh[user]), str(update[user])))
    if txt == 'uc':
        updatetradables(l[user], u[user])
        time.sleep(1)
        await message.channel.send('[' + str(l[user]) + ' ' + str(u[user]) + '] ' + str(exp[user]) + ' Days ' + 'Fresh=' + str(fresh[user]) + ' UpdatePositions=' + str(update[user]))
        lencond=uc(user)
        await message.channel.send(str(lencond)+' Condors Updated')
        td.kws.close()
        return
    if message.content.startswith('p'):
        print(txt)
        try:
            if txt=='positions':
                await message.channel.send(file=discord.File(printpositionsplot(user)))
                return
            num=int(txt.replace('p',''))
            await message.channel.send(file=discord.File(printplot(num,user)))
        except Exception as e:
            print(e)
            pass
    if message.content.startswith('*'):
        updatetradables(l[user], u[user])
        td.kws.connect()
        time.sleep(1)
        if txt=='*optch':
            await message.channel.send('[' + str(l[user]) + ' ' + str(u[user]) + '] ')
            print("Creating Option Chain")
            sf=td.pd.DataFrame()
            last_price=[]
            last_trade_time=[]
            volume=[]
            buy_quantity=[]
            sell_quantity=[]
            change=[]
            oi=[]
            strike=[]
            type=[]
            lt=td.last_tick
            for token in list(lt.keys()):
                last_price.append(lt[token]['last_price'])
                last_trade_time.append(lt[token]['last_trade_time'])
                volume.append(lt[token]['volume'])
                buy_quantity.append(lt[token]['buy_quantity'])
                sell_quantity.append(lt[token]['sell_quantity'])
                change.append(lt[token]['change'])
                oi.append(lt[token]['oi'])
                try:
                    strike.append(int(td.nstp(td.ibt(token),'strike')))
                except:
                    strike.append(0)
                type.append(td.nstp(td.ibt(token), 'instrument_type'))
            sf['last_price']=last_price
            sf['last_trade_time']=last_trade_time
            sf['volume']=volume
            sf['buy_quantity']=buy_quantity
            sf['sell_quantity']=sell_quantity
            sf['change']=change
            sf['oi']=oi
            sf['strike']=strike
            sf['type']=type
            print(sf)
            sf=sf.sort_values(by=['strike'])
            sf.to_csv('plots/optionchain.csv',index=False)
            await message.channel.send(file=discord.File('plots/optionchain.csv'))
            td.kws.close()
            return










