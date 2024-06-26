import pandas as pd
import numpy as np
import json
import networkx as nx
import matplotlib.pyplot as plt
import scraping as sc
import dataset as ds
import graph as gr


def main(filename, load_matrix=True, load_data=True, filter_list=None, centrality=False):
    if load_data:
        df = ds.dataset_loop(loop=False)
    else:
        df = ds.dataset_loop(loop=True) 
    if load_matrix:
        adjacency_matrix = gr.load_matrix(df, filter_list=filter_list, filename=filename)
    else:           
        adjacency_matrix = gr.matrix_creation(df, filter_list=filter_list, filename=filename)
    
    G = gr.graph_creation(adjacency_matrix)
    
    if centrality:
        centrality_measures = gr.centrality(G, df)
        return G, centrality_measures
    else:
        return G

if __name__ == "__main__":
    G = main(filename='adjacency_matrix_costituzione.npy', filter_list=['/costituzione'])
    plt.figure(figsize=(10, 10))
    pos = nx.spring_layout(G)
    nx.draw(G, with_labels=True, pos=pos, node_size=100, font_size=8, font_color='black', edge_color='gray', node_color='blue')

    plt.savefig('data/costituzione.png')
    
    
    	

    