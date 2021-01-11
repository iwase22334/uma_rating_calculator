
## 初期型全部入り

02_indiscriminate/
    分類なし、全種別込みレーティング


## 初期型レース種別入り１

03_shibaonly/
04_dirtonly/


## 初期型レース種別入り２

途中再開機能付き?

05_shiba_tan/
06_dirt_tan/


## 

上位３位までが勝利点を得る
- 10_shiba_tan/
- 11_dirt_tan/
- 12_syogai_tan/

```
k_factor_first = 16
k_factor_second = 8
k_factor_third = 4

expect = lambda rating_op, rating: 1.0 / (1 + pow(10, (rating_op - rating) / 400.0 ))
reword = lambda k_factor, actual, expect: k_factor * (actual - expect)

expect_sum += expect(rating_op, rating)
actual_sum += 1 if int(jyuni) < int(jyuni_op) else 0

rating_diff = reword(k_factor_first, 0, expect(rating_first, rating))\
              + reword(k_factor_second, 0, expect(rating_second, rating))\
              + reword(k_factor_third, 0, expect(rating_third, rating))\

new_rating = rating + rating_diff
```

## 

上位３位以外も順位に応じてポイントを得られる
- 20_shibaonly/
- 21_dirtonly/

```
k_factor_first = 16
k_factor_second = 12
k_factor_third = 8
k_factor_other = 4

expect = lambda rating_op, rating: 1.0 / (1 + pow(10, (rating_op - rating) / 400.0 ))
reword = lambda k_factor, actual, expect: k_factor * (actual - expect)

expect_sum += expect(rating_op, rating)
actual_sum += 1 if int(jyuni) < int(jyuni_op) else 0

rating_diff = reword(k_factor_first, 0, expect(rating_first, rating))\
              + reword(k_factor_second, 0, expect(rating_second, rating))\
              + reword(k_factor_third, 0, expect(rating_third, rating))\

new_rating = rating + rating_diff
```

