#include "blimit.hpp"
#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <vector>
#include <queue>
#include <sstream>
#include <string>
#include <mutex>
#include <thread>
#include <atomic>
#include <algorithm>
#include <chrono>

using namespace std;

static const int P = 7;

struct node;
struct edge;
struct cmp {
    bool operator()(const edge &a, const edge &b) const;
};

/*
 * Struktura wierzcholka
 */
struct node {
    int original_id;
    unsigned int original_b;
    unsigned int b;
    unsigned int sorted_part;
    unsigned int iterated;
    bool is_in_queue;
    unsigned int db;
    mutex mutS;
    mutex mutDB;
    priority_queue<edge, vector<edge>, cmp> adorators;
    vector<edge> nbr;
};

/*
 * Struktura krawedzi
 */
struct edge {
    int weight;
    node* to;

    edge(int w, node* v) : weight(w), to(v) {};

    bool operator<(edge e) const {
        return weight < e.weight ||
            (weight == e.weight && to->original_id < e.to->original_id);
    }
};

/*
 * operator() wykorzystywany w komperatorze do kolejki wierzcholkow adorujacych
 */
bool cmp::operator()(const edge &a, const edge &b) const {return b < a;}

/*
 * Zwraca najmniejsza krawedz jezeli kolejka adorujacych jest pelna
 * w.p.p krawedz(0,null)
 */
edge lastVal(node *v) {
    if(v->adorators.size() == v->original_b)
        return v->adorators.top();
    return edge(0, NULL);
}

/*
 * Wklada krawedz do kolejki adorujacych wierzcholek v,
 * Usuwa z niej najmniejsza jezeli kolejka sie przepelni
 */
void myInsert(node* v, edge e) {
    if(lastVal(v).weight > 0)
       v->adorators.pop();
    v->adorators.push(e);
}

/*
 * Znajduje najlepsza krawedz wychodzaca z v mogaca adorowac inny wierzcholek
 */
edge argmax(node *v) {
    while(v->iterated < v->nbr.size()) {
        if(v->sorted_part == 0) {
            int amount = min((int)v->nbr.size() - v->iterated, v->original_b * 7) - 1;
            partial_sort(v->nbr.begin() + v->iterated,
             v->nbr.begin() + v->iterated + amount, v->nbr.end(), cmp());
            v->sorted_part = amount - 1;
        }

        edge i = v->nbr[v->iterated];
        v->iterated++;
        v->sorted_part--;
        if(i.to->original_b != 0) {
            i.to->mutS.lock();

            if(lastVal(i.to) < edge(i.weight, v)) 
                return i; 

            i.to->mutS.unlock();
        }
    }

    return edge(0, NULL);
}

/*
 * funkcja pobierajaca z kolejki q, chronionej mutexem defQ 
 * wierzcholki ktorym bedzie probowac znalezc wierzcholki do adorowania,
 * modyfikuje wynik res, oraz vector nQ ktory bedzie wykorzystany do 
 * utworzenia kolejki q w kolejnej iteracji
 */
void match(queue<node*> *q, vector<node*> *nQ, mutex *defQ, atomic<int> *res) {
    while(1) {
        defQ->lock();
        if(q->empty()) {
            defQ->unlock();
            break;
        }
        node* v = q->front();
        v->is_in_queue = false;
        q->pop();
        defQ->unlock();

        v->mutDB.lock();
        v->b = v->db;
        v->db = 0;
        v->mutDB.unlock();

        unsigned int i = 1;
        while(i <= v->b) {
            edge x = argmax(v);    
            if(x.to != NULL) {
                i++;
                *res += x.weight;

                edge e = lastVal(x.to);
                if(e.to != NULL) {  
                    *res -= e.weight;
                    e.to->mutDB.lock();
                    e.to->db++;
                    e.to->mutDB.unlock();
                    nQ->push_back(e.to);
                }

                myInsert(x.to, edge(x.weight, v));
                x.to->mutS.unlock(); 
            }
            else break;   
        }

        v->b = 0;
    }
}

/*
 * Funkcja wykonywujaca algorytm b-adoratorow na grafie grapg 
 * i thread_count watkach
 */
int bAdorators(const vector<node*> &graph, int thread_count) {
    queue<node*> q;
    atomic<int> res{0};
    mutex defQ;
    for(auto i :graph)
        q.push(i);

    while(!q.empty()) {
        vector<node*> nextQ[thread_count];
        vector<thread> t;

        for(int i = 0; i < thread_count - 1; i++)
            t.push_back(thread(match, &q, &nextQ[i], &defQ, &res));

        match(&q, &nextQ[thread_count - 1], &defQ, &res);

        for(auto& i : t)
            i.join();
        for(auto nq : nextQ) {
            for(auto v : nq) {
                if(!v->is_in_queue) {
                    v->is_in_queue = true;
                    q.push(v);
                }
            }
        }
    }

    return res / 2;
}

/*
 * funckcja wczytujaca graf z pliku
 */
vector<node*> readInput(string file_name) {
    ifstream myfile(file_name);
    map<int, int> new_id;
    vector<node*> graph;
    int n = 0;

    if(myfile.is_open()) {
        while(1) {
            string line;
            while(myfile.peek() == '#')
                myfile.ignore(numeric_limits<streamsize>::max(), '\n');
    
            if(!getline(myfile, line)) break;
            int a, b, weight;
            stringstream ss(line);;
            ss >> a >> b >> weight;
            if(new_id.count(a) == 0) {
                new_id[a] = n;
                node* v = new node();
                v->original_id = a;
                graph.push_back(v);
                ++n;
            }
            if(new_id.count(b) == 0) {
                new_id[b] = n;
                node* v = new node();
                v->original_id = b;
                graph.push_back(v);
                ++n;
            }
            graph[new_id[a]]->nbr.push_back(
                edge(weight, graph[new_id[b]]));
            graph[new_id[b]]->nbr.push_back(
                edge(weight, graph[new_id[a]]));
        }
    }
    myfile.close();

   return graph;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "usage: "<<argv[0]<<" thread-count inputfile b-limit"<< endl;
        return 1;
    }

    int thread_count = stoi(argv[1]);
    int b_limit = stoi(argv[3]);
    string input_filename{argv[2]};

    vector<node*> graph = readInput(input_filename);

    auto start = std::chrono::high_resolution_clock::now(); 
    for (int b_method = 0; b_method < b_limit + 1; b_method++) {
        for(auto i : graph) {
            i->b = bvalue(b_method, i->original_id);
            i->original_b = i->b;
            i->db = i->b;
            i->is_in_queue = true;
            i->sorted_part = 0;
            i->iterated = 0;
            i->adorators = priority_queue<edge, vector<edge>, cmp>();
        } 

        cout<<bAdorators(graph, thread_count)<<endl;
    }
    auto finish = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> difference = finish- start;
    cerr<<difference.count();
    for(auto i : graph) delete i;
}