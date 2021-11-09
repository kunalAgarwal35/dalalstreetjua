import time

import discord
import ticker4discord as td
import matplotlib as plt
plt.use('Agg')
interactive : False
showcondor = []
condorev = {}
resultdf = td.pd.DataFrame()
l = td.l
u = td.u
exp = 2
fresh = 0
update = 1
client = discord.Client()
def changesubscriptions(newtokenslist):
    print("Should Receive ",len(newtokenslist))
    oldsub=list(td.last_tick.keys())
    td.kws.close()
    td.kws.unsubscribe(oldsub)
    td.kws.subscribe(newtokenslist)
    td.kws.set_mode(td.kws.MODE_FULL,newtokenslist)
    td.kws.connect(threaded=1)
def updatetradables(lstrike,ustrike):
    newtokenslist=[]
    instru=td.tradables(td.allinstruments,lstrike,ustrike)
    for ins in instru:
        newtokenslist.append(ins['instrument_token'])
    changesubscriptions(newtokenslist)


def updateparameter(txt):
    global l, u, exp, fresh, update
    if 'minstrike' in txt:
        l = int(txt.replace('$minstrike', ''))
        return
    if 'maxstrike' in txt:
        u = int(txt.replace('$maxstrike', ''))
        return
    if 'exp' in txt:
        exp = int(txt.replace('$exp', ''))
        return
    if 'fresh' in txt:
        fresh = int(txt.replace('$fresh', ''))
        return
    if 'update' in txt:
        update = int(txt.replace('$update', ''))
        return
def uc():
    global l, u, exp, fresh, update, showcondor, condorev, resultdf
    condorev, showcondor, resultdf = td.updatecondor(l, u, exp, fresh, update)
def printplot(n):
    print("Plotting Condor No: ",str(n))
    keys=list(condorev.keys())
    name = keys[n]
    print(name)
    pop=str(int(100*(resultdf[name] >= 0).sum() / len(resultdf[name])))
    contracts=name.replace('[','').replace(']','').replace('&',',').replace('-',',').replace(' ','').split(',')
    nname='['+contracts[0][-7:]+'-'+contracts[1][-7:]+']'+' & '+'['+contracts[2][-7:]+'-'+contracts[3][-7:]+']'
    resultdf[nname]=resultdf[name]
    pop = str(int(100 * (resultdf[nname] >= 0).sum() / len(resultdf[nname])))
    figure=resultdf.plot(x="spot", y=nname, grid=1,label='PnL',title=nname+' POP:'+pop+'%').get_figure().savefig('plots/'+name+'.png')
    return 'plots/'+name+'.png'
def printpositionsplot(resultdf):
    print('Plotting Current Positions')
    pop = str(int(100 * (resultdf['positions'] >= 0).sum() / len(resultdf['positions'])))
    figure=resultdf.plot(x='spot', y='positions', grid=1, label='PnL', title='Positions'+' POP:'+pop+'%').get_figure().savefig('plots/' + 'positions' + '.png')
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
    txt = message.content
    if message.content.startswith('$'):
        if 'minstrike' in txt:
            updateparameter(txt)
            await message.channel.send('Min Strike updated to: ' + str(l))
            return
        if 'maxstrike' in txt:
            updateparameter(txt)
            await message.channel.send('Max Strike updated to: ' + str(u))
            return
        if 'exp' in txt:
            updateparameter(txt)
            await message.channel.send('Days to expiry updated to: ' + str(exp))
            return
        if 'fresh' in txt:
            updateparameter(txt)
            await message.channel.send('Look for Fresh Condors updated to: ' + str(fresh))
            return
        if 'update' in txt:
            updateparameter(txt)
            await message.channel.send('Check for new positions updated to: ' + str(update))
            return
    if txt == 'uc':
        td.kws.connect(threaded=1)
        time.sleep(1)
        await message.channel.send('[' + str(l) + ' ' + str(u) + '] ' + str(exp) + ' Days ' + 'Fresh=' + str(
            fresh) + ' UpdatePositions=' + str(update))
        condorev,showcondor, resultdf = td.updatecondor(l, u, exp, fresh, update)
        await message.channel.send(str(len(condorev))+' Condors Updated')
        td.kws.close()
        return
    if message.content.startswith('p'):
        print(txt)
        try:
            if txt=='positions':
                await message.channel.send(file=discord.File(printpositionsplot(resultdf)))
                return
            num=int(txt.replace('p',''))
            await message.channel.send(file=discord.File(printplot(num)))
        except Exception as e:
            print(e)
            pass
    if message.content.startswith('*'):
        updatetradables(l, u)
        td.kws.connect()
        time.sleep(1)
        if txt=='*optch':
            await message.channel.send('[' + str(l) + ' ' + str(u) + '] ')
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










