import xml.etree.ElementTree as ET


def findNode(node, nodename):
    res = None
    print(f"  findNode: {node.tag} {len(node)}")
    for child in node:
        print(f"    child tag: {child.tag}")
        if child.tag == nodename:
            res = child
        else:
            if len(child) > 0:
                res = findNode(child, nodename)
        if res is not None:
            break
    return res


def findNodes(node, nodename, res):
    for child in node:
        if child.tag == nodename:
            res.append(child)
        else:
            if len(child) > 0:
                findNodes(child, nodename, res)

def getValue(node, names):
    res = None
    aname = node.attrib["NAME"]
    if aname in names:
        res = node.attrib["VALUE"]
    print(f"  getValue {aname} {res}")
    return res

with open("MOB_CDRATOR.XML","r",encoding="iso-8859-15") as fh:
    doc = ET.parse(fh)
    root = doc.getroot()
    print("got root")
    mcnt = 0
    sql_qry_cnt = 0
    for child in root.iter():
       if(child.tag == "MAPPING"):
           mnam = child.attrib["NAME"]
           mcnt += 1
           sql_qry_fnd = False
           print(f"mapping {child.tag} {mnam}")
           if len(child) > 0:
               tas = []
               findNodes(child, "TABLEATTRIBUTE", tas)
               for ta in tas:
                    val = getValue(ta,["Sql Query"])
                    if sql_qry_fnd == False:
                        if val is not None:
                            if len(val) > 0:
                                sql_qry_cnt += 1
                                sql_qry_fnd = True
           if sql_qry_fnd:
               print("  has Sql Query")

#    trafos = root.findall('./FOLDER/MAPPING')
#    print(len(trafos))
    print(mcnt)
    print(sql_qry_cnt)




