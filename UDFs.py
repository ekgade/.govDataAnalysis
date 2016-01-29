from collections import defaultdict
import sys
import re

@outputSchema("URLs:chararray")
def pickURLs(url):
   try:
      # these can be arbitrary regular expressions
      keyURLs = [
      'state\.gov',
      'treasury\.gov',
      'defense\.gov',
      'dod\.gov',
      'usdoj\.gov',
      'doi\.gov',
      'usda\.gov',
      'commerce\.gov',
      'dol\.gov',
      'hhs\.gov',
      'dot\.gov',
      'energy\.gov',
      'ed\.gov',
      'va\.gov',
      'dhs\.gov',
      'whitehouse\.gov',
      '\.senate\.gov',
      '\.house\.gov',
      'federalreserve\.gov',
      'cftc\.gov',
      'fdic\.gov',
      'fasb\.org',
      'ffiec\.gov',
      'fhfa\.gov',
      'fhfb\.gov',
      'finra\.org',
      'treasury\.gov\/initiatives\/fsoc\/Pages\/home\.aspx',
      'fdic\.gov',
      'fslic',
      'ncua\.gov',
      'occ\.gov',
      'ofheo\.gov',
      'financialresearch\.gov',
      'treasury\.gov',
      'treasury\.gov\/about\/history\/Pages\/ots\.aspx',
      'financialresearch\.gov',
      'sec\.gov',
      'sipc\.org',
      'consumerfinance\.gov',
      'ftc\.gov',
      'ginniemae\.gov',
      'portal\.hud\.gov\/hudportal\/HUD\?src\=\/federal\_housing\_administration',
      'fanniemae\.com',
      'freddiemac\.com',
      'makinghomeaffordable\.gov',
      'rd\.usda\.gov',
      'homeloans\.va\.gov',
      'benefits\.va\.gov\/homeloans',
      'fhfb\.gov',
      '\.senate\.gov',
      '\.house\.gov',
      'whitehouse\.gov',
      'state\.gov',
      'doj\.gov',
      'doe\.gov',
      'faa\.gov',
      'fema\.gov',
      'hhs\.gov',
      'cdc\.gov',
      'usphs\.gov',
      'treasury\.gov',
      'secretservice\.gov',
      'atf\.gov',
      'justice\.gov',
      'fbi\.gov',
      'cia\.gov',
      'defense\.gov',
      'dod\.gov',
      'nsa\.gov',
      'nctc\.gov',
      'epa\.gov',
      'ice\.gov',
      'noaa\.gov',
      'eia\.gov',
      'doe\.gov',
      'epa\.gov',
      'globalchange\.gov',
      'usaid\.gov',
      'usda\.gov',
      'hhs\.gov',
      'fws\.gov',
      'usgs\.gov',
      'nps\.gov',
      'dot\.gov',
      'doi\.gov',
      'fedcenter\.gov',
      'nasa\.gov',
      'fema\.gov',
      'gao\.gov',
      'gsa\.gov',
      'climate\.gov',
      'state\.gov',
      'cia\.gov',
      'cftc\.gov',
      'nsf\.gov',
      'justice\.gov'
      '\/'
      ]

      #URLs =  defaultdict(int) ##### WHAT DO THEY EQUAL EMILY???   defaultdict(int)
      URLs = []
         #URLs['other'] = 0

      for i in range(len(keyURLs)):
         tmp = len(re.findall(keyURLs[i], url, re.IGNORECASE))
         if tmp > 0:
            #URLs[keyURLs[i]] = tmp
            #URLs.append(keyURLs[i])
            return keyURLs[i]
      return 'other'
   except IOError:
      print('An error occured trying to read the file.')
   except ValueError:
      print('Non-numeric data found in the file.')
   except ImportError:
      print "NO module found"
   except EOFError:
      print('Why did you do an EOF on me?')
   except KeyboardInterrupt:
      print('You cancelled the operation.')
   except:
      print('An error occured.')

@outputSchema("counts:bag{tuple(word:chararray,count:int)}")
def Threat_countWords(content):
   try:
      # these can be arbitrary regular expressions
      Threat_Words = [
            '(*terror*)',
            '(9\/11*)',
            '(abuse\sof\spublic\soffice)'
            '(acidification)',
            '(adaptation)',
            '(adjustable\-rate\smortgages)',
            '(al\-qa*)',
            '(alien\ssmuggl*)',
            '(alternative\senergy)',
            '(anthropoc*)',
            '(anthropog*)',
            '(arms)',
            '(arms\sprolifer*)',
            '(arms\ssmuggl*)',
            '(arms\stransfer*)',
            '(assassin*)',
            '(atrocit*)',
            '(attack*)',
            '(authoritarian\smilitary\sextremists)',
            '(authoritarian\spopul*)',
            '(bailout*)',
            '(ballistic\smissile)',
            '(bankrupt*)',
            '(bin\sladen)',
            '(biological\sweapon*)',
            '(biopreparedness)',
            '(bioregulator*)',
            '(biosecurity)', 
            '(bioterror*)',
            '(border\ssecurity)',
            '(breaches\sof\sconstitutional\snorm*)',
            '(bubble)',
            '(capital)',
            '(capital\srequirement*)',
            '(carbon)',
            '(cartel*)',
            '(catastrophic\shealth\sevent*)',
            '(cdo)',
            '(cfc)',
            '(chemical\sweapon*)',
            '(clean\senergy)',
            '(climate)',
            '(climate\schange)',
            '(climategate)',
            '(co2)',
            '(collapse)',
            '(collateralize*)',
            '(conflict*)',
            '(conservation)',
            '(conservatorship)',
            '(conventional\sarm*)',
            '(correction)',
            '(corrupt*)',
            '(counterfeit*)',
            '(counterterror*)',
            '(coup*)',
            '(criminal\sbaron*)',
            '(criminal\senterpris*)',
            '(criminal\snetwork*)',
            '(crisis)',
            '(critical\sinfrastructure)',
            '(cyber\-attack*)',
            '(cyber\sattack*)',
            '(cyberattack)',
            '(cybersecurit*)',
            '(cyberterror*)',
            '(cyberwar*)',
            '(cyberwarfare)',
            '(debt)',
            '(default)',
            '(desertification)',
            '(dirty\sbomb)',
            '(disease*)',
            '(drug\straffic*)',
            '(dual\-use\sgood*)',
            '(electronic\swar*)',
            '(emergenc*)',
            '(emission*)',
            '(energy\sefficiency)',
            '(ethnic\sconflict*)',
            '(exposure)',
            '(failure*)',
            '(fannie\smae)',
            '(financial\sfraud)',
            '(fissile\smaterial)',
            '(food\ssecurity)',
            '(foreclosure)',
            '(forest\sconservation)',
            '(fragile\sstate*)',
            '(fraud*)',
            '(freddie\smac)',
            '(fresh\swater)',
            '(fundamentalis*)',
            '(gender\-based\sviolence)',
            '(genocide*)',
            '(ginnie\smae)',
            '(global\swarm*)',
            '(greenhouse)',
            '(gse)',
            '(haircut)',
            '(hedg*)',
            '(hijack*)',
            '(hockey\sstick)',
            '(home\sprice*)',
            '(hostile\sstate*)',
            '(human\srights\sabuse*)',
            '(human\srights\sviolat*)',
            '(hunger)',
            '(hydrocarbon*)',
            '(identity\sfraud)',
            '(ied)',
            '(if\syou\ssee\ssomething\,\ssay\ssomething)',
            '(illegal\smigration)',
            '(immigration)',
            '(improvised\sexplosive\sdevice*)',
            '(inflation*)',
            '(insolvent)',
            '(insurgen*)',
            '(intergovernmental\spanel\son\sclimate\schange)',
            '(international\sorganized\scrime)',
            '(ipcc)',
            '(irresponsible\sstate*)',
            '(jihad)',
            '(known\sand\ssuspected\sterror*)',
            '(ksts)',
            '(kyoto)',
            '(landmine*)',
            '(lehman)',
            '(leverag*)',
            '(liquidity)',
            '(losses)',
            '(major\sdisaster*)',
            '(marine\spollution)',
            '(mass\scasualt*)',
            '(massive\scasualt*)',
            '(meteorological)',
            '(methane)',
            '(military\sforce*)',
            '(mitigation)',
            '(money\slaunder*)',
            '(mortgage*)',
            '(mortgage\-backed)',
            '(narco\-traffic*)',
            '(narcotic*)',
            '(natural\sdisaster*)',
            '(non\-state\sactor*)',
            '(north\skorea)',
            '(nuclear)',
            '(ozone)',
            '(pandemic*)',
            '(panic)',
            '(plunge)',
            '(pollution)',
            '(poverty)',
            '(predatory)',
            '(proliferat*)',
            '(proliferation)',
            '(radiological)',
            '(receivership)',
            '(risk)',
            '(sea\slevel\srise)',
            '(sea\ssurface)',
            '(securitiz*)',
            '(security)',
            '(september\s11*)',
            '(shadow)',
            '(sluggish\seconomic\sgrowth)',
            '(smuggling)',
            '(solvency)',
            '(speculat*)',
            '(stockpille\ssecurity)',
            '(sub\-prime)',
            '(subprime)',
            '(suspicious\sactivity)',
            '(swap*)',
            '(systemic\srisk)',
            '(taliban)',
            '(terrorism)',
            '(terrorist)',
            '(threat*)',
            '(toxic)',
            '(toxins)',
            '(trafficking)',
            '(transnational\scriminal\sthreats)',
            '(transnational\sorganized\scrime)',
            '(transnational\sthreat*)',
            '(transparency)',
            '(unfcc)',
            '(united\snations\sframework\sconvention\son\sclimate\schange)',
            '(violat[a-z]+?\s?o?f?\sinternational\s?humanitarian\slaw)',
            '(violat[a-z]+?\s?o?f?\su?n?i?v?e?r?s?a?l?\s?human\sright)',
            '(violent\sconflict)',
            '(violent\sextremis*)',
            '(war)',
            '(warming)',
            '(weapon[a-z]+?\sof\smass\sdestruction)',
            '(whistleblower*)',
            '(wmd)',
            '(zoonotic\sdisease*)'
                 ]

      threat_counts  = defaultdict(int)
      threat_counts['total'] = 0

      if not content or not isinstance(content, unicode):
         return [(('total'), 0)]
      threat_counts['total'] = len(content.split())

      for i in range(len(Threat_Words)):
         tmp = len(re.findall(Threat_Words[i], content, re.IGNORECASE))
         if tmp > 0:
            threat_counts[Threat_Words[i]] = tmp
      # Convert counts to bag
      countBag = []
      for word in threat_counts.keys():
         countBag.append( (word, threat_counts[word] ) )
      return countBag
   except IOError:
      print('An error occured trying to read the file.')
   except ValueError:
      print('Non-numeric data found in the file.')
   except ImportError:
      print "NO module found"
   except EOFError:
      print('Why did you do an EOF on me?')
   except KeyboardInterrupt:
      print('You cancelled the operation.')
   except:
      print('An error occured.')

@outputSchema("counts:bag{tuple(year:int, month:int, word:chararray, count:int, filled:int, afterLast:int, URLs:chararray)}")
#@outputSchema("counts:bag{tuple(year:int, month:int, word:chararray, count:int, seendates:int, date:int, URLs:chararray)}")

def fillInCounts(data):
   try:
      outBag = []
      firstYear = 2013
      firstMonth = 9
      lastYear = 0
      lastMonth = 0
      # used to compute averages for months with multiple captures
      # word -> (year, month) -> count
      counts = defaultdict(lambda : defaultdict(list))

      # The next two dictionaries areused to return stats for subsequent months without captures
      # (year,month) -> date
      lastCaptureOfMonth = defaultdict(int)
      # word -> (year,month) -> {date, count}
      endOfMonthCounts = defaultdict(lambda : defaultdict(lambda: dict({'date':0,'count':0})))
      seenDates = {}
      #for s in src:
      #  maxSeenDate= max(date)
      # maxSeenDate=data[data.src==src].max(date) ### try filter relevant rows, then ask for max observed date
      for (src, date, wordCounts, urls) in data:
#         maxSeenDate = max(data[src]['date'])
         for (word, countTmp) in wordCounts:
            year = int(date[0:4])
            month = int(date[4:6])
            # some regexs '(chemical\,?\sbiological\,?\so?r?\s?a?n?d?\s?nuclear\s?w?e?a?p?o?n?)'
            # are returning tuples of the form (chemical\,), with no count
            # not sure what's going on, this is temporary fix
            if isinstance(countTmp,str) or isinstance(countTmp,int):
               count = int(countTmp)
            else:
               continue

            ymtup = (year, month)
            counts[word][ymtup].append(count)

            if date > lastCaptureOfMonth[ymtup]:
               lastCaptureOfMonth[ymtup] = date
            if date > endOfMonthCounts[word][ymtup]['date']:
               endOfMonthCounts[word][ymtup]['date'] = date
               endOfMonthCounts[word][ymtup]['count'] = count

            seenDates[(year,month)] = True

            if year < firstYear:
               firstYear = year
               firstMonth = month
            elif year == firstYear and month < firstMonth:
               firstMonth = month
            elif year > lastYear:
               lastYear = year
               lastMonth = month
            elif year == lastYear and month > lastMonth:
               lastMonth = month


      for word in counts.keys():
         # The data was collected until Sep 2013
         years = range(firstYear, 2014)
         useCount = 0
         afterLast = False
         filled = False
         ymLastUsed = (0,0)
         for y in years:
            if y > lastYear:
               afterLast = True
            if y == firstYear:
               mStart = firstMonth
            else:
               mStart = 1
            if y == 2013:
               mEnd = 9
            else:
               mEnd = 12
            for m in range(mStart, mEnd+1):
               if y == lastYear and m > lastMonth:
                  #afterLast = True
          ## trying to fix the problem of having years
               #if afterLast == True:
                  pass
               #else:
              #    continue
               if (y,m) in seenDates:
                  # Output sum, as we will divide by sum of totals later
                  useCount = sum(counts[word][(y,m)])
                  ymLastUsed = (y,m)
                  filled = False
               else:
                  # If we didn't see this date in the capture, we want to use the last capture
                  # we saw previously (we might have two captures in Feb,
                  # so for Feb we output both, but to fill-in for March we would only output
                  # the final Feb count)

                  # Automatically output an assumed total for each month (other words
                  # may no longer exist)
                  #if word == 'total':
                  #    useCount = counts[word][ymLastUsed]
                  #elif
                  if endOfMonthCounts[word][ymLastUsed]['date'] == lastCaptureOfMonth[ymLastUsed]:
                     useCount = endOfMonthCounts[word][ymLastUsed]['count']
                  else:
                     continue
                  filled = True
               if useCount == 0:
                  continue
               outBag.append( (y, m, word, useCount, int(filled), int(afterLast), urls) )
               #outBag.append( (urlname, urlcount, y, m, word, useCount, int(filled), int(afterLast)) )

      return outBag
   except IOError:
      print('An error occured trying to read the file.')
   except ValueError:
      print('Non-numeric data found in the file.')
   except ImportError:
      print "NO module found"
   except EOFError:
      print('Why did you do an EOF on me?')
   except KeyboardInterrupt:
      print('You cancelled the operation.')
   except:
      print('An error occurred.')
###########
