package com.polytech.graph;


import java.util.LinkedList;

public class Graph {

    class ENode {
        int num;
        int weight = 0;
        ENode nextEdge;

        ENode() {
            num = 0;
            nextEdge = null;
        }

        ENode(int num) {
            this.num = num;
            nextEdge = null;
        }

        ENode(int num, int weight) {
            this.num = num;
            this.weight = weight;
            nextEdge = null;
        }
    }

    class VNode {
        char vertex;
        ENode firstEdge = null;
    }

    class Edge {
        char start;
        char end;
        int weight;

        Edge() {

        }

        Edge(Edge edge) {
            start = edge.start;
            end = edge.end;
            weight = edge.weight;
        }

        Edge(char start, char end, int weight) {
            this.start = start;
            this.end = end;
            this.weight = weight;
        }

    }

    VNode[] mVertex;
    Edge[] medges;
    int edgeNum = 0;
    int[][] mWeights;
    static int INF = Integer.MAX_VALUE;

    public int vertexToNum(char c) {
        int res = 0;
        for (int i = 0; i < mVertex.length; i++) {
            if (mVertex[i].vertex == c)
                return i;
        }
        return res;
    }

    public Graph(char[] vexs, char[][] edges) {
        edgeNum = edges.length;
        mVertex = new VNode[vexs.length];
        for (int i = 0; i < vexs.length; i++) {
            mVertex[i] = new VNode();
            mVertex[i].vertex = vexs[i];
        }
        for (int i = 0; i < edges.length; i++) {
            ENode temp = new ENode(vertexToNum(edges[i][1]));

            if (mVertex[vertexToNum(edges[i][0])].firstEdge == null) {
                mVertex[vertexToNum(edges[i][0])].firstEdge = temp;
            } else {
                ENode p = mVertex[vertexToNum(edges[i][0])].firstEdge;

                while (p.nextEdge != null) {
                    p = p.nextEdge;
                }
                p.nextEdge = temp;
            }
        }
    }

    Graph(char[] vexs, char[][] edges, int[] weights, Boolean isDirected) {
        edgeNum = edges.length;
        medges = new Edge[edgeNum];

        mWeights = new int[vexs.length][vexs.length];
        for (int i = 0; i < vexs.length; i++) {
            for (int j = 0; j < vexs.length; j++) {
                mWeights[i][j] = INF;
            }
        }

        mVertex = new VNode[vexs.length];
        for (int i = 0; i < vexs.length; i++) {
            mVertex[i] = new VNode();
            mVertex[i].vertex = vexs[i];
        }
        for (int i = 0; i < edges.length; i++) {
            ENode temp = new ENode(vertexToNum(edges[i][1]), weights[i]);
            medges[i] = new Edge(edges[i][0], edges[i][1], weights[i]);
            mWeights[vertexToNum(edges[i][0])][vertexToNum(edges[i][1])] = weights[i];

            if (mVertex[vertexToNum(edges[i][0])].firstEdge == null) {
                mVertex[vertexToNum(edges[i][0])].firstEdge = temp;
            } else {
                ENode p = mVertex[vertexToNum(edges[i][0])].firstEdge;

                while (p.nextEdge != null) {
                    p = p.nextEdge;
                }
                p.nextEdge = temp;
            }

            if (!isDirected) {
                temp = new ENode(vertexToNum(edges[i][0]), weights[i]);
                mWeights[vertexToNum(edges[i][1])][vertexToNum(edges[i][0])] = weights[i];

                if (mVertex[vertexToNum(edges[i][1])].firstEdge == null) {
                    mVertex[vertexToNum(edges[i][1])].firstEdge = temp;
                } else {
                    ENode p = mVertex[vertexToNum(edges[i][1])].firstEdge;

                    while (p.nextEdge != null) {
                        p = p.nextEdge;
                    }
                    p.nextEdge = temp;
                }
            }

        }
    }


    /**
     * 广度优先遍历
     */
    public void BFS() {
        LinkedList<VNode> queue = new LinkedList<VNode>();
        Boolean[] isVisited = new Boolean[mVertex.length];
        for (int i = 0; i < mVertex.length; i++) {
            isVisited[i] = false;
        }
        queue.offer(mVertex[0]);
        System.out.print(mVertex[0].vertex + " ");
        isVisited[0] = true;
        while (!queue.isEmpty()) {
            VNode p = queue.remove();
            ENode q = p.firstEdge;
            while (q != null) {
                if (!isVisited[q.num]) {
                    isVisited[q.num] = true;
                    System.out.print(mVertex[q.num].vertex + " ");
                    queue.offer(mVertex[q.num]);
                }
                q = q.nextEdge;
            }
        }
        System.out.println();
    }

    /**
     * 深度优先遍历
     */
    public void DFSfirst(VNode vertex, Boolean[] isVisited) {
        int num = vertexToNum(vertex.vertex);
        if (!isVisited[num]) {
            isVisited[num] = true;
            System.out.print(vertex.vertex + " ");
        }
        ENode p = vertex.firstEdge;
        while (p != null) {
            DFSfirst(mVertex[p.num], isVisited);
            p = p.nextEdge;
        }
    }

    /**
     * 深度优先遍历
     */
    public void DFS() {
        Boolean[] isVisited = new Boolean[mVertex.length];
        for (int i = 0; i < mVertex.length; i++) {
            isVisited[i] = false;
        }
        for (int i = 0; i < mVertex.length; i++) {
            if (!isVisited[i])
                DFSfirst(mVertex[i], isVisited);
        }
        System.out.println();

    }


    //walk all the edges in the nondecreasing order of weight

    /**
     * 最小生成树算法
     * @return
     */
    public Edge[] Kruskal() {
        Edge[] res = new Edge[mVertex.length - 1];
        if (edgeNum < 1) return res;

        //bubble sort
        for (int i = 0; i < edgeNum - 1; i++) {
            for (int j = edgeNum - 1; j > i; j--) {
                if (medges[j].weight < medges[j - 1].weight) {
                    Edge temp;
                    temp = medges[j];
                    medges[j] = medges[j - 1];
                    medges[j - 1] = temp;
                }
            }
        }

        int j = 0;
        char[] vends = new char[mVertex.length];
        for (int i = 0; i < vends.length; i++) {
            vends[i] = mVertex[i].vertex;
        }
        for (int i = 0; i < edgeNum; i++) {
            int s = vertexToNum(medges[i].start);
            int e = vertexToNum(medges[i].end);
            if (vends[s] != vends[e]) {
                char temp_c = vends[s];
                for (int k = 0; k < vends.length; k++) {
                    if (vends[k] == temp_c)
                        vends[k] = vends[e];
                }
                res[j] = medges[i];
                j++;
            }
        }

        int length = 0;
        for (int i = 0; i < res.length; i++)
            length += res[i].weight;
        System.out.printf("Kruskal=%d: ", length);
        for (int i = 0; i < res.length; i++)
            System.out.printf("(%c,%c) ", res[i].start, res[i].end);
        System.out.printf("\n");
        return res;
    }

    /**
     * 最小生成树算法
     * @param c
     */
    public void Prim(char c) {
        int pos = vertexToNum(c);
        char[] res = new char[mVertex.length];
        int[] key = new int[mVertex.length];
        int[] front = new int[mVertex.length];
        int minKey = INF, minPos = 0;
        for (int i = 0; i < mVertex.length; i++) {
            key[i] = mWeights[pos][i];
            front[i] = -1;
            if (key[i] < minKey) {
                minKey = key[i];
                minPos = i;
            }
        }
        front[minPos] = pos;
        key[pos] = 0;
        res[0] = mVertex[pos].vertex;


        int i = 0, k = 1, sum = 0;
        while (i++ < mVertex.length - 1) {
            minKey = INF;
            for (int j = 0; j < mVertex.length; j++) {
                if (key[j] != 0 && key[j] < minKey) {
                    minPos = j;
                    minKey = key[j];
                }
            }
            res[k++] = mVertex[minPos].vertex;
            sum = sum + mWeights[front[vertexToNum(res[k - 1])]][vertexToNum(res[k - 1])];

            key[minPos] = 0;
            for (int j = 0; j < mVertex.length; j++) {
                if (key[j] != 0 && mWeights[minPos][j] < key[j]) {
                    key[j] = mWeights[minPos][j];
                    front[j] = minPos;
                }
            }
        }

        System.out.printf("PRIM(%c)=%d: ", mVertex[pos].vertex, sum);
        for (i = 0; i < mVertex.length; i++)
            System.out.printf("%c ", res[i]);
        System.out.printf("\n");
    }


    public Boolean BellmanFord(char c) {
        int s = vertexToNum(c);
        int[] d = new int[mVertex.length];
        int[] front = new int[mVertex.length];

        for (int i = 0; i < mVertex.length; i++) {
            d[i] = INF;
            front[i] = -1;
        }
        d[s] = 0;

        for (int i = 0; i < mVertex.length - 1; i++) {
            for (int j = 0; j < edgeNum; j++) {
                int start = vertexToNum(medges[j].start);
                int end = vertexToNum(medges[j].end);
                if (d[end] > d[start] + mWeights[start][end]) {
                    d[end] = d[start] + mWeights[start][end];
                    front[end] = start;
                }
            }
        }

        for (int i = 0; i < edgeNum; i++) {
            int start = vertexToNum(medges[i].start);
            int end = vertexToNum(medges[i].end);
            if (d[end] > d[start] + mWeights[start][end]) {
                return false;
            }
        }

        System.out.printf("BellmanFord:  ");
        for (int i = 0; i < mVertex.length; i++) {
            if (i == s) continue;
            System.out.print(mVertex[front[i]].vertex + " ");
        }
        System.out.println();
        return true;
    }

    public void Dijkstra(char c) {
        int s = vertexToNum(c);
        int[] S = new int[mVertex.length];
        int[] d = new int[mVertex.length];
        int[] front = new int[mVertex.length];

        for (int i = 0; i < mVertex.length; i++) {
            d[i] = INF;
            S[i] = -1;
            front[i] = -1;
        }
        d[s] = 0;

        //use array d[] to implement PriorityQueue
        int minPos = s, minD = INF;
        for (int i = 0; i < mVertex.length; i++) {
            minD = INF;
            for (int j = 0; j < mVertex.length; j++) {
                if (d[j] != 0 && d[j] < minD) {
                    minD = d[j];
                    minPos = j;
                }
            }
            d[minPos] = 0;

            int start = minPos;
            int end = 0;
            ENode p = mVertex[start].firstEdge;
            while (p != null) {
                end = p.num;
                if (d[end] > d[start] + mWeights[start][end]) {
                    d[end] = d[start] + mWeights[start][end];
                    front[end] = start;
                }
                p = p.nextEdge;
            }

        }

        System.out.printf("Dijkstra:  ");
        for (int i = 0; i < mVertex.length; i++) {
            if (i == s) continue;
            System.out.print(mVertex[front[i]].vertex + " ");
        }
        System.out.println();

    }

    public void printGraph() {
        for (int i = 0; i < mVertex.length; i++) {
            System.out.printf("%d%c:", i, mVertex[i].vertex);
            ENode p = mVertex[i].firstEdge;
            while (p != null) {
                System.out.printf("%d%c(%d) ", p.num, mVertex[p.num].vertex, p.weight);
                p = p.nextEdge;
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
       /* char[] vexs = {'A', 'B', 'C', 'D', 'E', 'F', 'G'};
        char[][] edges = new char[][]{
                {'A', 'C'},
                {'A', 'D'},
                {'A', 'E'},
                {'B', 'C'},
                {'B', 'D'},
                {'B', 'F'},
                {'C', 'D'},
                {'C', 'F'}};
        Graph mgraph = new Graph(vexs, edges);
        mgraph.printGraph();
        mgraph.BFS();
        mgraph.DFS();*/


        char[] vexs2 = {'A', 'B', 'C', 'D', 'E', 'F', 'G'};
        char[][] edges2 =
                {
                        {'A', 'B'},
                        {'A', 'F'},
                        {'A', 'G'},
                        {'B', 'C'},
                        {'B', 'F'},
                        {'C', 'D'},
                        {'C', 'E'},
                        {'C', 'F'},
                        {'D', 'E'},
                        {'E', 'F'},
                        {'E', 'G'},
                        {'F', 'G'},
                };
        int[] weight2 = {12, 16, 14, 10, 7, 3, 5, 6, 4, 2, 8, 9};

        Graph mgraph2 = new Graph(vexs2, edges2, weight2, false);
        mgraph2.printGraph();
        mgraph2.Kruskal();
        mgraph2.Prim('A');

        /*char[] vexs3 = {'s', 't', 'x', 'y', 'z'};
        char[][] edges3 =
                {
                        {'s', 't'},
                        {'s', 'y'},
                        {'t', 'x'},
                        {'t', 'y'},
                        {'t', 'z'},
                        {'x', 't'},
                        {'y', 'x'},
                        {'y', 'z'},
                        {'z', 's'},
                        {'z', 'x'}
                };

        int[] weights3 = {6, 7, 5, 8, -4, -2, -3, 9, 2, 7};

        Graph mgraph3 = new Graph(vexs3, edges3, weights3, true);
        mgraph3.printGraph();
        mgraph3.BellmanFord('s');

        char[] vexs4 = {'s', 't', 'x', 'y', 'z'};
        char[][] edges4 =
                {
                        {'s', 't'},
                        {'s', 'y'},
                        {'t', 'x'},
                        {'t', 'y'},
                        {'x', 'z'},
                        {'y', 't'},
                        {'y', 'x'},
                        {'y', 'z'},
                        {'z', 's'},
                        {'z', 'x'}
                };
        int[] weights4 = {10, 5, 1, 2, 4, 3, 9, 2, 7, 6};
        Graph mgraph4 = new Graph(vexs4, edges4, weights4, true);
        mgraph4.Dijkstra('s');
        mgraph4.BellmanFord('s');*/
    }
}
