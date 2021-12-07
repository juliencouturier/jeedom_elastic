import argparse
from lxml import etree as ET


def merge_attrib(node, merge_node):
    for key, val in merge_node.attrib.items():
        if val != '' and node.attrib.get(key, '')=='':
            node.attrib[key] = val

def merge_Manufacturer(node, merge_node):
    manufacturer_node = node.find('{http://code.google.com/p/open-zwave/}Manufacturer')
    manufacturer_merge_node = merge_node.find('{http://code.google.com/p/open-zwave/}Manufacturer')
    if manufacturer_merge_node is not None:
        if manufacturer_node is None:
            node.insert(0, manufacturer_merge_node)
        else:
            merge_attrib(manufacturer_node, manufacturer_merge_node)
            product_node = manufacturer_node.find('{http://code.google.com/p/open-zwave/}Product')
            product_merge_node = manufacturer_merge_node.find('{http://code.google.com/p/open-zwave/}Product')
            if product_node is None:
                manufacturer_node.append(product_merge_node)
            else:
                merge_attrib(product_node, product_merge_node)

def merge_CommandClasses(node, merge_node):
    node_cc = node.find('{http://code.google.com/p/open-zwave/}CommandClasses')
    merge_cc = merge_node.find('{http://code.google.com/p/open-zwave/}CommandClasses')
    if merge_cc is not None:
        if node_cc is None:
            node.append(merge_cc)
        else:            
            node_cc_dict = {cc.attrib['id'] : cc for cc in node_cc.findall('{http://code.google.com/p/open-zwave/}CommandClass')}
            for cc in merge_cc.findall('{http://code.google.com/p/open-zwave/}CommandClass'):
                my_cc = node_cc_dict.get(cc.attrib['id'])
                if my_cc is None:
                    node_cc.append(cc)
                else:
                    merge_CommandClass(my_cc, cc)

def merge_CommandClass(node, merge_node):
    merge_attrib(node, merge_node)
    node_instance_dict = {ins.attrib['index'] : ins for ins in node.findall('{http://code.google.com/p/open-zwave/}Instance')}
    for merge_ins in merge_node.findall('{http://code.google.com/p/open-zwave/}Instance'):
        node_ins = node_instance_dict.get(merge_ins.attrib['index'])
        if node_ins is None:
            node.append(merge_ins)
        else:
            merge_instance(node_ins, merge_ins)

    node_value_dict = {(val.attrib['instance'], val.attrib['index']) : val for val in node.findall('{http://code.google.com/p/open-zwave/}Value')}
    for merge_val in merge_node.findall('{http://code.google.com/p/open-zwave/}Value'):
        node_val = node_value_dict.get((merge_val.attrib['instance'], merge_val.attrib['index']))
        if node_val is None:
            node.append(merge_val)
        else:
            merge_value(node_val, merge_val)

    node_sensormap_dict = {(sensor.attrib['index'], sensor.attrib['type']) : sensor for sensor in node.findall('{http://code.google.com/p/open-zwave/}SensorMap')}
    for merge_sensor in merge_node.findall('{http://code.google.com/p/open-zwave/}SensorMap'):
        node_sensor = node_sensormap_dict.get((merge_sensor.attrib['index'], merge_sensor.attrib['type']))
        if node_sensor is None:
            node.append(merge_sensor)
        else:
            merge_sensor_map(node_sensor, merge_sensor)

    node_asso_dict = {asso.attrib['num_groups'] : asso for asso in node.findall('{http://code.google.com/p/open-zwave/}Associations')}
    for merge_asso in merge_node.findall('{http://code.google.com/p/open-zwave/}Associations'):
        node_asso = node_asso_dict.get(merge_asso.attrib['num_groups'])
        if node_asso is None:
            node.append(merge_asso)
        else:
            merge_associaions(node_asso, merge_asso)


def merge_instance(node, merge_node):
    merge_attrib(node, merge_node)

def merge_value(node, merge_node):
    merge_attrib(node, merge_node)
    help_merge_node = merge_node.find('{http://code.google.com/p/open-zwave/}Help')
    if help_merge_node is not None:
        help_node = node.find('{http://code.google.com/p/open-zwave/}Help')
        if help_node is None:
            node.append(help_merge_node)
    node_item_dict = {item.attrib['value'] : item for item in node.findall('{http://code.google.com/p/open-zwave/}Item')}
    for item in merge_node.findall('{http://code.google.com/p/open-zwave/}Item'):
        node_item = node_item_dict.get(item.attrib['value'])
        if node_item is None:
            node.append(item)
        else:
            merge_attrib(node_item, item)


def merge_sensor_map(node, merge_node):
    merge_attrib(node, merge_node)

def merge_associaions(node, merge_node):
    merge_attrib(node, merge_node)
    node_group_dict = {item.attrib['index'] : item for item in node.findall('{http://code.google.com/p/open-zwave/}Group')}
    for group in merge_node.findall('{http://code.google.com/p/open-zwave/}Group'):
        node_group = node_group_dict.get(group.attrib['index'])
        if node_group is None:
            node.append(group)
        else:
            merge_attrib(node_group, group)
            merge_group_node(node_group, group)
    # Add Group --> Node
    
def merge_group_node(node, merge_node):
    node_groupnode_dict = {item.attrib['id'] : item for item in node.findall('{http://code.google.com/p/open-zwave/}Node')}
    for groupnode in merge_node.findall('{http://code.google.com/p/open-zwave/}Node'):
        node_groupnode = node_groupnode_dict.get(groupnode.attrib['id'])
        if node_groupnode is None:
            node.append(groupnode)
        else:
            merge_attrib(node_groupnode, groupnode)


def main(*args, **kwargs):
    input = ET.parse(kwargs['input'])
    merge = input = ET.parse(kwargs['merge'])
    input_nodes = {
        node.attrib['id'] : node for node in input.findall('{http://code.google.com/p/open-zwave/}Node')
    }
    for merge_node in merge.findall('{http://code.google.com/p/open-zwave/}Node'):
        node_id = merge_node.attrib['id']
        node = input_nodes.get(node_id)
        if node is None:
            input_nodes.append(merge_node)
        else:
            merge_attrib(node, merge_node)
            merge_Manufacturer(node, merge_node)
            merge_CommandClasses(node, merge_node)
    kwargs['output'].write(ET.tostring(input, encoding='utf-8', pretty_print=True))

        


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Merge jeedom ZWave')
    parser.add_argument('-i', '--input', type=argparse.FileType('r'), required=True,
                        help='Input File')
    parser.add_argument('-m', '--merge', type=argparse.FileType('r'), required=True,
                        help='Merge File')
    parser.add_argument('-o', '--output', type=argparse.FileType('wb'), required=True,
                        help='Output File')
    args = parser.parse_args()
    main(**args.__dict__)