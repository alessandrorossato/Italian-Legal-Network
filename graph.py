import pandas as pd
import numpy as np
import json
import networkx as nx
import matplotlib.pyplot as plt
import dataset as ds


def load_matrix(df, path="data/matrix/", filename="adjacency_matrix.npy", filter_list=None):
    """
    Load the adjacency matrix from a specified path and filename, and return it as a DataFrame.
    
    Args:
    df (pd.DataFrame): DataFrame containing node names.
    path (str): Path to the directory containing the adjacency matrix file.
    filename (str): Name of the file containing the adjacency matrix.

    Returns:
    pd.DataFrame: DataFrame representing the adjacency matrix.
    """
    adjacency_matrix = np.load(f"{path}{filename}")
    
    # Filter the DataFrame based on the filter_list
    if filter_list:
        df = ds.filter_list(df, filter_list)
        
    return pd.DataFrame(adjacency_matrix, columns=df["name"], index=df["name"])


def matrix_creation(df, save=True, path="data/matrix/", filter_list=None, filename="adjacency_matrix.npy"):
    """
    Create an adjacency matrix from a DataFrame of references and optionally save it to a file.
    
    Args:
    df (pd.DataFrame): DataFrame containing the references.
    save (bool): Whether to save the adjacency matrix to a file.
    path (str): Path to the directory where the matrix will be saved.
    filter_list (list): List of prefixes to filter references by.
    filename (str): Name of the file to save the adjacency matrix.

    Returns:
    pd.DataFrame: DataFrame representing the adjacency matrix.
    """
    # Filter the DataFrame based on the filter_list
    if filter_list:
        df = ds.filter_list(df, filter_list)

    # Initialize the adjacency matrix with zeros
    adjacency_matrix = np.zeros((len(df), len(df)))

    # Populate the adjacency matrix based on the references
    for i in range(len(df)):
        for ref in df["references"][i]:
            # If the reference does not exist in df, skip it
            if ref not in df["link"].values:
                continue
            else:
                adjacency_matrix[i, df[df["link"] == ref].index[0]] = 1

    # Save the adjacency matrix if required
    if save:
        np.save(f"{path}{filename}", adjacency_matrix)

    return pd.DataFrame(adjacency_matrix, columns=df["name"], index=df["name"])

def graph_creation(adjacency_matrix):
    """
    Create a graph from an adjacency matrix.
    
    Args:
    adjacency_matrix (pd.DataFrame): DataFrame representing the adjacency matrix.

    Returns:
    networkx.Graph: Graph created from the adjacency matrix.
    """
    return nx.from_numpy_array(adjacency_matrix.values)


def centrality(G, df):
    """
    Calculate centrality measures for a graph.
    
    Args:
    G (networkx.Graph): Graph for which centrality measures are to be calculated.
    df (pd.DataFrame): DataFrame containing node names.

    Returns:
    pd.DataFrame: DataFrame containing degree centrality, eigenvector centrality, and PageRank for each node.
    """
    # Calculate degree centrality
    degree_centrality = nx.degree_centrality(G)
    degree_centrality = pd.DataFrame.from_dict(
        degree_centrality, orient="index", columns=["degree_centrality"]
    )
    
    # Calculate eigenvector centrality
    eigenvector_centrality = nx.eigenvector_centrality(G)
    eigenvector_centrality = pd.DataFrame.from_dict(
        eigenvector_centrality, orient="index", columns=["eigenvector_centrality"]
    )

    # Calculate PageRank
    pagerank = nx.pagerank(G)
    pagerank = pd.DataFrame.from_dict(pagerank, orient="index", columns=["pagerank"])

    # Merge centrality measures into a single DataFrame
    centrality_measures = degree_centrality.merge(
        eigenvector_centrality, left_index=True, right_index=True
    )
    centrality_measures = centrality_measures.merge(
        pagerank, left_index=True, right_index=True
    )
    centrality_measures.index = df["name"]

    return centrality_measures


if __name__ == "__main__":
    # Load the dataset
    df = ds.dataset_loop(loop=False, sources_load=False)
    
    # Create the adjacency matrix
    adjacency_matrix = matrix_creation(df, save=True)
    
    # Create the graph
    G = graph_creation(adjacency_matrix)
    
    # Calculate centrality measures
    centrality_measures = centrality(G, df)
    
    # Optionally, you can save or visualize the centrality measures
    # Example: Save to a CSV file
    centrality_measures.to_csv("data/centrality_measures.csv")
    
    # Example: Print centrality measures
    print(centrality_measures)