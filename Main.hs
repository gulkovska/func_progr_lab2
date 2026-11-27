{-# LANGUAGE ScopedTypeVariables #-}

-- | Багатопотокове обмінне сортування зі злиттям Бетчера (Batcher's
--   merge-exchange sort).  Компілювати з ключем -threaded, запускати з
--   +RTS -N<RівеньПаралелізму> -RTS, щоб потоки виконувались на кількох ядрах.
module Main (main) where

import Control.Concurrent (MVar, forkIO, newEmptyMVar, putMVar, takeMVar)
import Control.Monad (forM_, replicateM)
import Data.Array.IO (IOUArray, getElems, newListArray, readArray, writeArray)
import Data.Bits ((.&.))
import Data.List (sort)

import System.IO (hFlush, stdout)
import Text.Read (readMaybe)

-- | Один етап мережі: список пар індексів, які треба порівняти й,
--   за потреби, обміняти.  У межах етапу пари не перетинаються, тому
--   їх можна виконувати одночасно без блокувань.
type Comparators = [(Int, Int)]

-- | Повідомлення для потоку-працівника: Nothing — завершення роботи.
type Job = Maybe Comparators

-- | Найбільший ступінь двійки, що не перевищує n.
highestPowerOfTwo :: Int -> Int
highestPowerOfTwo n = go 1
  where
    go p
      | p * 2 <= n = go (p * 2)
      | otherwise  = p

-- | Генерація всіх етапів мережі Бетчера для масиву довжини n.
--   Це ітеративна форма алгоритму M (Batcher's merge-exchange sort):
--     P = 2^(T-1), 2^(T-2), ..., 1;  T = floor(lg n)
--     R = 0, D = P
--     для Q = 2^(T-1), ..., P:
--        порівнюємо A[i] і A[i+D] для всіх i, де (i & P) == R
--        D = Q - P, R = P
batcherStages :: Int -> [Comparators]
batcherStages n = filter (not . null) (concatMap (roundsFor p0) ps)
  where
    p0 = highestPowerOfTwo n
    ps = takeWhile (> 0) (iterate (`quot` 2) p0)

    roundsFor q0 p = go q0 0 p
      where
        -- q — поточне Q, r — поточне R, d — поточне D.
        go q r d
          | r /= 0 && q == p = []
          | otherwise =
              let (qNext, dNext) =
                    if r == 0 then (q, d) else (q `quot` 2, q - p)
                  pairs = [ (i, i + dNext)
                          | i <- [0 .. n - dNext - 1]
                          , i .&. p == r
                          ]
              in pairs : go qNext p dNext

-- | Порівняння з обміном в масиві, що спільно використовується потоками.
exchange :: IOUArray Int Int -> (Int, Int) -> IO ()
exchange arr (i, j) = do
  a <- readArray arr i
  b <- readArray arr j
  if a > b
    then writeArray arr i b >> writeArray arr j a
    else pure ()

-- | Розбиття етапу на k неперетинних ділянок ( статичний баланс ).
splitWork :: Int -> [a] -> [[a]]
splitWork k xs = go k xs
  where
    (base, extra) = length xs `quotRem` k
    go 0 _ = []
    go cnt rest =
      let size = base + if cnt <= extra then 1 else 0
          (part, rest') = splitAt size rest
      in part : go (cnt - 1) rest'

-- | Потік-працівник: отримує чергову порцію порівнянь, виконує її
--   і сповіщає головний потік через MVar-бар'єр.
worker :: IOUArray Int Int -> MVar Job -> MVar () -> IO ()
worker arr inbox done = loop
  where
    loop = do
      job <- takeMVar inbox
      case job of
        Nothing -> putMVar done ()          -- сигнал завершення
        Just pairs -> do
          mapM_ (exchange arr) pairs
          putMVar done ()
          loop

-- | Сортування за допомогою рівно k потоків.
--   Повертає відсортований масив, кількість етапів і кількість порівнянь.
batcherSort :: Int -> [Int] -> IO ([Int], Int, Int)
batcherSort k xs
  | n <= 1 = pure (xs, 0, 0)
  | otherwise = do
      arr <- newListArray (0, n - 1) xs :: IO (IOUArray Int Int)
      inboxes <- replicateM k newEmptyMVar
      dones   <- replicateM k newEmptyMVar
      forM_ [0 .. k - 1] $ \i ->
        forkIO (worker arr (inboxes !! i) (dones !! i))

      forM_ stages $ \pairs -> do
        -- роздаємо роботу всім k потокам, потім чекаємо на всіх
        forM_ (zip inboxes (splitWork k pairs)) $ \(inbox, chunk) ->
          putMVar inbox (Just chunk)
        mapM_ takeMVar dones

      -- коректно завершуємо працівників
      mapM_ (`putMVar` Nothing) inboxes
      mapM_ takeMVar dones

      result <- getElems arr
      pure (result, length stages, sum (map length stages))
  where
    n = length xs
    stages = batcherStages n

-- | Безпечне перетворення рядка в Int: числа, що не поміщаються в Int,
--   вважаються помилковими (Read Int для Int може «перевернути» велике число).
parseInt :: String -> Maybe Int
parseInt s = do
  x <- readMaybe s :: Maybe Integer
  if x >= toInteger (minBound :: Int) && x <= toInteger (maxBound :: Int)
    then Just (fromInteger x)
    else Nothing

-- | Зчитування додатнього цілого числа з клавіатури.
readPositive :: String -> IO Int
readPositive msg = do
  putStr msg
  hFlush stdout
  line <- getLine
  case parseInt line of
    Just x | x > 0 -> pure x
    _ -> do
      putStrLn "Помилка: введіть ціле число, більше за 0."
      readPositive msg

-- | Зчитування n цілих чисел (можна вводити в один рядок або в кілька).
readNumbers :: Int -> IO [Int]
readNumbers = go []
  where
    go acc left
      | left <= 0 = pure (reverse acc)
      | otherwise = do
          putStr (if null acc then "Введіть елементи масиву: "
                              else "Недостатньо елементів, продовжте: ")
          hFlush stdout
          line <- getLine
          let tokens = words line
              parsed = [(t, parseInt t) | t <- tokens]
              bad = [t | (t, Nothing) <- parsed]
              vals = take left [v | (_, Just v) <- parsed]
          if null bad
            then pure ()
            else putStrLn $ "Увага: не вдалося зчитати як ціле Int: "
                         ++ unwords (take 3 bad)
                         ++ if length bad > 3 then " ..." else ""
          go (reverse vals ++ acc) (left - length vals)

main :: IO ()
main = do
  putStrLn "Обмінне сортування зі злиттям Бетчера (багатопотокове)"
  k <- readPositive "Кількість потоків k: "
  n <- readPositive "Кількість елементів n: "
  xs <- readNumbers n
  (ys, stageCount, comparisons) <- batcherSort k xs

  putStrLn $ "Вхідний масив:     " ++ unwords (map show xs)
  putStrLn $ "Відсортований:     " ++ unwords (map show ys)
  putStrLn $ "Перевірка:         " ++ if ys == sort xs then "OK" else "ПОМИЛКА"
  putStrLn $ "Потоків: " ++ show k
          ++ " | етапів: " ++ show stageCount
          ++ " | порівнянь: " ++ show comparisons
