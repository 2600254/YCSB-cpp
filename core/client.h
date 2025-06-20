//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <string>

#include "db.h"
#include "core_workload.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading, bool is_htap, bool is_ap,
                        bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, bool *ap_done, bool *time_limit) {

  try {
    if (init_db) {
      db->Init();
    }

    int ops = 0;
    auto start_time = std::chrono::high_resolution_clock::now();;
    for (int i = 0; i < num_ops; ++i) {
      if (rlim) {
        rlim->Consume(1);
      }
      if (is_htap) {
        if (is_ap) {
          if(time_limit && *time_limit) {
            break;
          }
          wl->DoAP(*db);
          auto now_time = std::chrono::high_resolution_clock::now();
          std::cout<< "AP operation done at "<< 
            std::chrono::duration_cast<std::chrono::microseconds>(
              now_time - start_time).count() << " microseconds" << std::endl;
        } else {
          if (ap_done && *ap_done) {
            break;
          }
          wl->DoTransaction(*db);
        }
      } else {
        if (is_loading) {
          wl->DoInsert(*db);
        } else {
          wl->DoTransaction(*db);
        }
      }
      ops++;
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    latch->CountDown();
    return ops;
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
