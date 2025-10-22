#include <iostream>
#include <string>
#include <fstream>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>
#include <array>
#include <cstring>
#include <mutex>
#include <vector>
#include "Car.h"
#include "final_Car.h"
#include <mpi.h>

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(Car, model, year, engine_l, result)
#define MAIN_THREAD 0
#define DATA_THREAD_RANK 1
#define RESULT_THREAD_RANK 2
#define FIRST_WORKER 3
#define NUM_WORKERS 4
#define DATA_TH_CAR_ARRAY_SIZE 10

using namespace std;
using json = nlohmann::json;

struct ByResult
{
    bool operator()(const Car &a, const Car &b) const
    {
        int c = std::strcmp(a.result, b.result);
        if (c != 0)
            return c < 0;

        c = std::strcmp(a.model, b.model);
        if (c != 0)
            return c < 0;

        if (a.year != b.year)
            return a.year < b.year;
        return a.engine_l < b.engine_l;
    }
};

string findFiveEqual(const string &s)
{
    if (s.empty())
        return "";
    int n = (int)s.size();

    for (int i = 0; i < n; ++i)
    {
        for (int j = i; j < n; ++j)
        {
            string sub = s.substr(i, j - i + 1);
            if ((int)sub.size() < 5)
                continue;
            int run = 1;
            for (int k = 1; k < (int)sub.size(); ++k)
            {
                if (sub[k] == sub[k - 1])
                {
                    ++run;
                    if (run == 5)
                    {
                        string cand = sub.substr(k - 4, 5);

                        bool allSame = true;
                        for (int t = 1; t < 5; ++t)
                        {
                            if (cand[t] != cand[0])
                            {
                                allSame = false;
                                break;
                            }
                        }
                        if (allSame)
                            return cand;
                    }
                }
                else
                {
                    run = 1;
                }
            }
        }
    }
    return "";
}

void send_car(Car &car, string th_name, int rank, int receiver_name)
{
    MPI_Send(&car, sizeof(car), MPI_BYTE, receiver_name, 0, MPI_COMM_WORLD);
    cout << th_name << " " << rank << ": sent car: " << car.model << " " << car.result << endl;
}

MPI_Status receive_car(Car &car, string th_name, int rank, int sender_name)
{
    MPI_Status status;
    MPI_Recv(&car, sizeof(car), MPI_BYTE, sender_name, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    cout << th_name << " " << rank << ": received car : " << car.model << " " << car.result << endl;
    return status;
}

void send_stop_signal(int receiver_name){
    Car stop;
    strncpy(stop.model, "STOP", sizeof(stop.model) - 1);
    strncpy(stop.result, "STOP", sizeof(stop.result) - 1);
    MPI_Send(&stop, sizeof(stop), MPI_BYTE, receiver_name, 1, MPI_COMM_WORLD);
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int rank; int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    cout << "Process: " << rank << " of " << size << endl;
    
    if (rank == MAIN_THREAD){
        cout << "MAIN: starting program." << endl;
        set<Car, ByResult> setRes;
        vector<Car> allCars;
        int allCarsC = 0;

        ifstream f("f3.json");
        json j;
        f >> j;
        f.close();
        for (auto &item : j)
        {
            Car c;
            string m = item["model"].get<string>();
            strncpy(c.model, m.c_str(), sizeof(c.model) - 1);
            c.model[sizeof(c.model) - 1] = '\0';
            c.year = item["year"].get<int>();
            c.engine_l = item["engine_l"].get<double>();
            string result = item.value("result", "");
            strncpy(c.result, result.c_str(), sizeof(c.result) - 1);
            c.result[sizeof(c.result) - 1] = '\0';
            allCars.push_back(c);
        }
        int idx = 0;
        for(Car car : allCars){
            int target = FIRST_WORKER + (idx % NUM_WORKERS);
            send_car(car, "MAIN", rank, target);
            idx++;
        }
        cout << "=========MAIN: SEND ALL CARS=========" << endl;

        for (int i = 0; i < NUM_WORKERS; ++i){
            int target = FIRST_WORKER + i;
            send_stop_signal(target);
        }
        cout << "=========MAIN: SEND STOP SIGNAL=========" << endl;

        MPI_Status main_status;
        while (true){
            Car car;
            main_status = receive_car(car, "MAIN", rank, RESULT_THREAD_RANK);
            if(main_status.MPI_TAG == 1)
                break;
            setRes.insert(car);
        }
        cout << "\n\n=========FINAL RESULT=========" << endl;
        for (Car car : setRes)
            cout << car.result << " " << car.model << " " << car.year << " " << car.engine_l << " " << endl;
        cout << "Total amount of cars is: " << setRes.size() << endl;
    } else if (rank == DATA_THREAD_RANK){
        // data thread
        // MPI::COMM_WORLD.Recv() 10 elements from main thread and save them to array with size 10
        // MPI::COMM_WORLD.Send() one element from array to worker thread
        array<Car, DATA_TH_CAR_ARRAY_SIZE> data_th_car_array;
        MPI_Status data_status;
        while (true)
        {
            data_status = receive_car();
            if(data_status.MPI_TAG == 1){
                break;
            }
            else if (data_status.MPI_TAG == 0 && data_th_car_array.size() - 1 != DATA_TH_CAR_ARRAY_SIZE)
            {
                //call from main. 
                //check if the array is not full.
                //receive the car and save it to the array.
            }
            else if(data_status.MPI_TAG == 2 && !data_th_car_array.empty()){
                //call from worker. 
                // check if array is not empty.
                //get the car from monitor and send it to worker.
            }
        }
    }
    else if (rank >= FIRST_WORKER && rank < FIRST_WORKER + NUM_WORKERS){
        MPI_Status work_status;
        while (true){
            Car car;
            work_status = receive_car(car, "WORKER", rank, MAIN_THREAD);
            if (work_status.MPI_TAG == 1) break;

            string s = findFiveEqual(car.model);
            if ('a' <= s[0] && 'z' >= s[0]){
                strncpy(car.result, s.c_str(), sizeof(car.result) - 1);
                send_car(car, "WORKER", rank, RESULT_THREAD_RANK);
            }
            else
                cout << "WORKER " << rank << ": skiped car: " << car.model << " " << s << endl;
        }
        cout << "=========WORKER: SEND ALL CARS=========" << endl;
        send_stop_signal(RESULT_THREAD_RANK);
    }
    else if (rank == RESULT_THREAD_RANK){
        set<Car, ByResult> setRes;
        MPI_Status result_status;
        int finished = 0;
        while (finished < NUM_WORKERS)
        {
            Car car;
            result_status = receive_car(car, "RESULT", rank, MPI_ANY_SOURCE);
            if (result_status.MPI_TAG == 1){
                finished++;
            } else {
                setRes.insert(car);
            }
        }
        cout << "=========RESULT RECEIVED ALL CARS=========" << endl;
        for (Car car : setRes)
            send_car(car, "RESULT", rank, MAIN_THREAD);
        send_stop_signal(MAIN_THREAD);
    }
    MPI_Finalize();
    return 0;
}
